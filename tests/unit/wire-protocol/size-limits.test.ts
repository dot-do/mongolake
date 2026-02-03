/**
 * Wire Protocol Size Limits Tests - GREEN Phase
 *
 * Tests for request/response size limits, document size validation,
 * batch operation limits, and streaming for large responses.
 *
 * These tests verify the implementation of size limit validation
 * in the MongoDB wire protocol module.
 */

import { describe, it, expect, beforeEach } from 'vitest';

import {
  SizeLimitValidator,
  SizeLimitError,
  StreamingResponseBuilder,
  createSizeLimitValidator,
  createStreamingResponseBuilder,
  isValidRequestSize,
  isValidDocumentSize,
  formatBytes,
  buildSizeLimitErrorResponse,
  MONGODB_MAX_DOCUMENT_SIZE,
  MONGODB_MAX_MESSAGE_SIZE,
  MONGODB_MAX_BATCH_COUNT,
  DEFAULT_SIZE_LIMITS,
  SizeLimitErrorCode,
  type SizeLimitConfig,
} from '../../../src/wire-protocol/size-limits';

import {
  parseMessage,
  parseMessageHeader,
  StreamingMessageParser,
  OpCode,
} from '../../../src/wire-protocol/message-parser';

import {
  serializeDocument,
  buildSuccessResponse,
  buildErrorResponse,
  buildCursorResponse,
  MongoErrorCode,
} from '../../../src/wire-protocol/bson-serializer';

import {
  MAX_WIRE_MESSAGE_SIZE,
  MIN_WIRE_MESSAGE_SIZE,
  MAX_BATCH_SIZE,
  MAX_BATCH_BYTES,
} from '../../../src/constants';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Create a Uint8Array of specified size (for testing)
 */
function createBuffer(size: number): Uint8Array {
  return new Uint8Array(size);
}

/**
 * Create an array of document sizes for batch testing
 */
function createDocumentSizes(count: number, sizeEach: number): number[] {
  return Array(count).fill(sizeEach);
}

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
  view.setInt32(0, messageLength, true);
  view.setInt32(4, requestId, true);
  view.setInt32(8, responseTo, true);
  view.setInt32(12, opCode, true);
  return new Uint8Array(buffer);
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
  let sectionsSize = 0;
  for (const section of sections) {
    if (section.type === 0) {
      sectionsSize += 1 + section.payload.length;
    } else {
      const idBytes = new TextEncoder().encode(section.identifier! + '\0');
      sectionsSize += 1 + 4 + idBytes.length + section.payload.length;
    }
  }

  const messageLength = 16 + 4 + sectionsSize;
  const buffer = new Uint8Array(messageLength);
  const view = new DataView(buffer.buffer);

  view.setInt32(0, messageLength, true);
  view.setInt32(4, requestId, true);
  view.setInt32(8, responseTo, true);
  view.setInt32(12, 2013, true);
  view.setUint32(16, flags, true);

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

// ============================================================================
// Default Constants Tests
// ============================================================================

describe('Size Limit Constants', () => {
  it('should have MongoDB-compatible document size limit (16MB)', () => {
    expect(MONGODB_MAX_DOCUMENT_SIZE).toBe(16 * 1024 * 1024);
  });

  it('should have MongoDB-compatible message size limit (48MB)', () => {
    expect(MONGODB_MAX_MESSAGE_SIZE).toBe(48 * 1024 * 1024);
  });

  it('should have MongoDB-compatible batch count limit (100,000)', () => {
    expect(MONGODB_MAX_BATCH_COUNT).toBe(100_000);
  });

  it('should have proper default configuration', () => {
    expect(DEFAULT_SIZE_LIMITS.maxRequestSize).toBe(16 * 1024 * 1024);
    expect(DEFAULT_SIZE_LIMITS.maxResponseSize).toBe(16 * 1024 * 1024);
    expect(DEFAULT_SIZE_LIMITS.maxDocumentSize).toBe(16 * 1024 * 1024);
    expect(DEFAULT_SIZE_LIMITS.maxBatchCount).toBe(100_000);
    expect(DEFAULT_SIZE_LIMITS.maxWireMessageSize).toBe(48 * 1024 * 1024);
    expect(DEFAULT_SIZE_LIMITS.strictMode).toBe(true);
  });

  it('should match constants from constants.ts', () => {
    expect(MONGODB_MAX_MESSAGE_SIZE).toBe(MAX_WIRE_MESSAGE_SIZE);
    expect(MONGODB_MAX_BATCH_COUNT).toBe(MAX_BATCH_SIZE);
  });

  it('should have correct error codes', () => {
    expect(SizeLimitErrorCode.RequestTooLarge).toBe(10334);
    expect(SizeLimitErrorCode.ResponseTooLarge).toBe(10335);
    expect(SizeLimitErrorCode.DocumentTooLarge).toBe(10334);
    expect(SizeLimitErrorCode.BatchTooLarge).toBe(10336);
    expect(SizeLimitErrorCode.BatchSizeTooLarge).toBe(10337);
    expect(SizeLimitErrorCode.MessageTooLarge).toBe(10338);
  });
});

// ============================================================================
// SizeLimitValidator Tests
// ============================================================================

describe('SizeLimitValidator', () => {
  let validator: SizeLimitValidator;

  beforeEach(() => {
    validator = new SizeLimitValidator();
  });

  describe('Configuration', () => {
    it('should use default configuration when none provided', () => {
      const config = validator.getConfig();
      expect(config.maxRequestSize).toBe(DEFAULT_SIZE_LIMITS.maxRequestSize);
      expect(config.maxDocumentSize).toBe(DEFAULT_SIZE_LIMITS.maxDocumentSize);
    });

    it('should allow custom configuration', () => {
      const customConfig: SizeLimitConfig = {
        maxRequestSize: 1024 * 1024,
        maxDocumentSize: 512 * 1024,
        maxBatchCount: 1000,
      };
      const customValidator = new SizeLimitValidator(customConfig);
      const config = customValidator.getConfig();

      expect(config.maxRequestSize).toBe(1024 * 1024);
      expect(config.maxDocumentSize).toBe(512 * 1024);
      expect(config.maxBatchCount).toBe(1000);
      expect(config.maxResponseSize).toBe(DEFAULT_SIZE_LIMITS.maxResponseSize);
    });

    it('should merge custom config with defaults', () => {
      const customValidator = new SizeLimitValidator({
        maxBatchCount: 500,
      });
      const config = customValidator.getConfig();

      expect(config.maxBatchCount).toBe(500);
      expect(config.maxRequestSize).toBe(DEFAULT_SIZE_LIMITS.maxRequestSize);
    });
  });

  describe('Request Validation', () => {
    it('should accept request within size limit', () => {
      const result = validator.validateRequest(1000);
      expect(result.valid).toBe(true);
      expect(result.actualSize).toBe(1000);
      expect(result.error).toBeUndefined();
    });

    it('should accept request at exactly the size limit', () => {
      const result = validator.validateRequest(MONGODB_MAX_DOCUMENT_SIZE);
      expect(result.valid).toBe(true);
    });

    it('should reject request exceeding size limit', () => {
      const size = MONGODB_MAX_DOCUMENT_SIZE + 1;
      const result = validator.validateRequest(size);

      expect(result.valid).toBe(false);
      expect(result.error).toBeInstanceOf(SizeLimitError);
      expect(result.error?.code).toBe(SizeLimitErrorCode.RequestTooLarge);
      expect(result.error?.codeName).toBe('RequestTooLarge');
      expect(result.error?.actualSize).toBe(size);
      expect(result.error?.maxSize).toBe(MONGODB_MAX_DOCUMENT_SIZE);
    });

    it('should reject request exceeding wire message limit', () => {
      const size = MONGODB_MAX_MESSAGE_SIZE + 1;
      const result = validator.validateRequest(size);

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe(SizeLimitErrorCode.MessageTooLarge);
      expect(result.error?.codeName).toBe('MessageTooLarge');
    });

    it('should accept Uint8Array input', () => {
      const buffer = createBuffer(1000);
      const result = validator.validateRequest(buffer);

      expect(result.valid).toBe(true);
      expect(result.actualSize).toBe(1000);
    });

    it('should reject oversized Uint8Array input', () => {
      const buffer = createBuffer(MONGODB_MAX_DOCUMENT_SIZE + 100);
      const result = validator.validateRequest(buffer);

      expect(result.valid).toBe(false);
      expect(result.error).toBeDefined();
    });
  });

  describe('Response Validation', () => {
    it('should accept response within size limit', () => {
      const result = validator.validateResponse(5000);
      expect(result.valid).toBe(true);
    });

    it('should reject response exceeding size limit', () => {
      const size = MONGODB_MAX_DOCUMENT_SIZE + 1;
      const result = validator.validateResponse(size);

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe(SizeLimitErrorCode.ResponseTooLarge);
      expect(result.error?.codeName).toBe('ResponseTooLarge');
    });

    it('should reject response exceeding wire message limit', () => {
      const size = MONGODB_MAX_MESSAGE_SIZE + 1;
      const result = validator.validateResponse(size);

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe(SizeLimitErrorCode.MessageTooLarge);
    });
  });

  describe('Document Validation', () => {
    it('should accept document within size limit', () => {
      const result = validator.validateDocument(1024 * 1024);
      expect(result.valid).toBe(true);
    });

    it('should accept document at exactly the limit', () => {
      const result = validator.validateDocument(MONGODB_MAX_DOCUMENT_SIZE);
      expect(result.valid).toBe(true);
    });

    it('should reject document exceeding size limit', () => {
      const size = MONGODB_MAX_DOCUMENT_SIZE + 1;
      const result = validator.validateDocument(size);

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe(SizeLimitErrorCode.DocumentTooLarge);
      expect(result.error?.codeName).toBe('BSONObjectTooLarge');
      expect(result.error?.limitType).toBe('document');
    });

    it('should accept Uint8Array document input', () => {
      const buffer = createBuffer(500);
      const result = validator.validateDocument(buffer);

      expect(result.valid).toBe(true);
      expect(result.actualSize).toBe(500);
    });
  });

  describe('Batch Validation', () => {
    it('should accept batch within count limit', () => {
      const docs = createDocumentSizes(100, 1000);
      const result = validator.validateBatch(docs);

      expect(result.valid).toBe(true);
      expect(result.documentCount).toBe(100);
      expect(result.totalSize).toBe(100000);
      expect(result.oversizedDocuments).toHaveLength(0);
    });

    it('should accept batch at exactly count limit', () => {
      const docs = createDocumentSizes(MONGODB_MAX_BATCH_COUNT, 100);
      const result = validator.validateBatch(docs);

      expect(result.valid).toBe(true);
      expect(result.documentCount).toBe(MONGODB_MAX_BATCH_COUNT);
    });

    it('should reject batch exceeding count limit', () => {
      const docs = createDocumentSizes(MONGODB_MAX_BATCH_COUNT + 1, 100);
      const result = validator.validateBatch(docs);

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe(SizeLimitErrorCode.BatchTooLarge);
      expect(result.error?.codeName).toBe('TooManyDocuments');
    });

    it('should detect oversized documents in batch', () => {
      const docs = [
        1000,
        MONGODB_MAX_DOCUMENT_SIZE + 1,
        2000,
        MONGODB_MAX_DOCUMENT_SIZE + 500,
        3000,
      ];
      const result = validator.validateBatch(docs);

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe(SizeLimitErrorCode.DocumentTooLarge);
      expect(result.oversizedDocuments).toContain(1);
      expect(result.oversizedDocuments).toContain(3);
    });

    it('should reject batch exceeding total size limit', () => {
      const customValidator = new SizeLimitValidator({
        maxBatchSize: 1024 * 1024,
      });
      const docs = createDocumentSizes(100, 20000);
      const result = customValidator.validateBatch(docs);

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe(SizeLimitErrorCode.BatchSizeTooLarge);
      expect(result.error?.codeName).toBe('BatchTooLarge');
    });

    it('should calculate total size correctly', () => {
      const docs = [100, 200, 300, 400, 500];
      const result = validator.validateBatch(docs);

      expect(result.totalSize).toBe(1500);
      expect(result.documentCount).toBe(5);
    });

    it('should accept Uint8Array documents in batch', () => {
      const docs = [
        createBuffer(100),
        createBuffer(200),
        createBuffer(300),
      ];
      const result = validator.validateBatch(docs);

      expect(result.valid).toBe(true);
      expect(result.totalSize).toBe(600);
    });

    it('should handle empty batch', () => {
      const result = validator.validateBatch([]);

      expect(result.valid).toBe(true);
      expect(result.documentCount).toBe(0);
      expect(result.totalSize).toBe(0);
    });

    it('should handle single document batch', () => {
      const result = validator.validateBatch([5000]);

      expect(result.valid).toBe(true);
      expect(result.documentCount).toBe(1);
      expect(result.totalSize).toBe(5000);
    });
  });

  describe('Batch Fit Calculation', () => {
    it('should calculate how many documents fit in size limit', () => {
      const sizes = [1000, 1000, 1000, 1000, 1000];
      const count = validator.calculateBatchFit(sizes, 3000);

      expect(count).toBe(3);
    });

    it('should include at least one document even if oversized', () => {
      const sizes = [5000, 1000, 1000];
      const count = validator.calculateBatchFit(sizes, 3000);

      expect(count).toBe(1);
    });

    it('should handle exact fit', () => {
      const sizes = [1000, 1000, 1000];
      const count = validator.calculateBatchFit(sizes, 3000);

      expect(count).toBe(3);
    });

    it('should return all documents if they fit', () => {
      const sizes = [100, 200, 300, 400];
      const count = validator.calculateBatchFit(sizes, 10000);

      expect(count).toBe(4);
    });

    it('should use default target size when not specified', () => {
      const sizes = createDocumentSizes(100, 100000);
      const count = validator.calculateBatchFit(sizes);

      expect(count).toBeLessThanOrEqual(42);
      expect(count).toBeGreaterThan(0);
    });
  });

  describe('Streaming Decision', () => {
    it('should not recommend streaming for small responses', () => {
      expect(validator.shouldStream(1024 * 1024)).toBe(false);
    });

    it('should recommend streaming for large responses', () => {
      expect(validator.shouldStream(10 * 1024 * 1024)).toBe(true);
    });

    it('should recommend streaming at cursor batch target size', () => {
      const config = validator.getConfig();
      expect(validator.shouldStream(config.cursorBatchTargetSize + 1)).toBe(true);
    });

    it('should not recommend streaming below cursor batch target size', () => {
      const config = validator.getConfig();
      expect(validator.shouldStream(config.cursorBatchTargetSize - 1)).toBe(false);
    });
  });
});

// ============================================================================
// SizeLimitError Tests
// ============================================================================

describe('SizeLimitError', () => {
  it('should create error with correct properties', () => {
    const error = new SizeLimitError(
      'Document too large',
      SizeLimitErrorCode.DocumentTooLarge,
      'BSONObjectTooLarge',
      20000000,
      16777216,
      'document'
    );

    expect(error.message).toBe('Document too large');
    expect(error.code).toBe(SizeLimitErrorCode.DocumentTooLarge);
    expect(error.codeName).toBe('BSONObjectTooLarge');
    expect(error.actualSize).toBe(20000000);
    expect(error.maxSize).toBe(16777216);
    expect(error.limitType).toBe('document');
    expect(error.name).toBe('SizeLimitError');
  });

  it('should generate valid error response', () => {
    const error = new SizeLimitError(
      'Request too large',
      SizeLimitErrorCode.RequestTooLarge,
      'RequestTooLarge',
      20000000,
      16777216,
      'request'
    );

    const response = error.toErrorResponse(1, 0);

    expect(response).toBeInstanceOf(Uint8Array);
    expect(response.length).toBeGreaterThan(0);

    const parsed = parseMessage(response);
    expect(parsed.type).toBe('OP_MSG');
  });

  it('should be instanceof Error', () => {
    const error = new SizeLimitError(
      'Test error',
      SizeLimitErrorCode.MessageTooLarge,
      'MessageTooLarge',
      100,
      50,
      'test'
    );

    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(SizeLimitError);
  });
});

// ============================================================================
// StreamingResponseBuilder Tests
// ============================================================================

describe('StreamingResponseBuilder', () => {
  let builder: StreamingResponseBuilder;

  beforeEach(() => {
    builder = new StreamingResponseBuilder();
  });

  describe('Small Response', () => {
    it('should build single response for small data set', () => {
      const documents = [{ x: 1 }, { x: 2 }, { x: 3 }];
      const sizes = [100, 100, 100];

      const result = builder.buildResponse(
        1, 0,
        'testdb.users',
        documents,
        sizes
      );

      expect(result.hasMore).toBe(false);
      expect(result.cursorId).toBe(0n);
      expect(result.includedCount).toBe(3);
      expect(result.remainingCount).toBe(0);
      expect(result.response).toBeInstanceOf(Uint8Array);
    });

    it('should return valid wire protocol message', () => {
      const documents = [{ name: 'test' }];
      const sizes = [50];

      const result = builder.buildResponse(
        1, 0,
        'testdb.users',
        documents,
        sizes
      );

      const parsed = parseMessage(result.response);
      expect(parsed.type).toBe('OP_MSG');
    });
  });

  describe('Large Response (Streaming)', () => {
    it('should stream large responses via cursor', () => {
      const customBuilder = new StreamingResponseBuilder(
        new SizeLimitValidator({
          cursorBatchTargetSize: 1000,
        })
      );

      const documents = Array(100).fill({ data: 'x'.repeat(100) });
      const sizes = Array(100).fill(150);

      const result = customBuilder.buildResponse(
        1, 0,
        'testdb.users',
        documents,
        sizes
      );

      expect(result.hasMore).toBe(true);
      expect(result.cursorId).not.toBe(0n);
      expect(result.includedCount).toBeLessThan(100);
      expect(result.remainingCount).toBeGreaterThan(0);
      expect(result.includedCount + result.remainingCount).toBe(100);
    });

    it('should provide custom cursor ID generator', () => {
      const customBuilder = new StreamingResponseBuilder(
        new SizeLimitValidator({
          cursorBatchTargetSize: 500,
        })
      );

      const documents = Array(50).fill({ x: 1 });
      const sizes = Array(50).fill(100);

      const customCursorId = 999999n;
      const result = customBuilder.buildResponse(
        1, 0,
        'testdb.users',
        documents,
        sizes,
        () => customCursorId
      );

      if (result.hasMore) {
        expect(result.cursorId).toBe(customCursorId);
      }
    });
  });

  describe('GetMore Response', () => {
    it('should build getMore response for cursor continuation', () => {
      const customBuilder = new StreamingResponseBuilder(
        new SizeLimitValidator({
          cursorBatchTargetSize: 500,
        })
      );

      const documents = Array(50).fill({ x: 1 });
      const sizes = Array(50).fill(100);
      const cursorId = 12345n;

      const result = customBuilder.buildGetMoreResponse(
        2, 0,
        cursorId,
        'testdb.users',
        documents,
        sizes,
        0
      );

      expect(result.response).toBeInstanceOf(Uint8Array);
      expect(result.includedCount).toBeGreaterThan(0);
    });

    it('should set cursorId to 0 on last batch', () => {
      const documents = [{ x: 1 }, { x: 2 }];
      const sizes = [50, 50];
      const cursorId = 12345n;

      const result = builder.buildGetMoreResponse(
        2, 0,
        cursorId,
        'testdb.users',
        documents,
        sizes,
        0
      );

      expect(result.cursorId).toBe(0n);
      expect(result.hasMore).toBe(false);
      expect(result.includedCount).toBe(2);
    });

    it('should handle startIndex for pagination', () => {
      const customBuilder = new StreamingResponseBuilder(
        new SizeLimitValidator({
          cursorBatchTargetSize: 200,
        })
      );

      const documents = Array(20).fill({ x: 1 });
      const sizes = Array(20).fill(100);

      const result = customBuilder.buildGetMoreResponse(
        2, 0,
        12345n,
        'testdb.users',
        documents,
        sizes,
        5
      );

      expect(result.includedCount + result.remainingCount).toBe(15);
    });
  });
});

// ============================================================================
// Factory Functions Tests
// ============================================================================

describe('Factory Functions', () => {
  describe('createSizeLimitValidator', () => {
    it('should create validator with default config', () => {
      const validator = createSizeLimitValidator();
      expect(validator).toBeInstanceOf(SizeLimitValidator);
      expect(validator.getConfig().maxRequestSize).toBe(MONGODB_MAX_DOCUMENT_SIZE);
    });

    it('should create validator with custom config', () => {
      const validator = createSizeLimitValidator({
        maxRequestSize: 1024 * 1024,
      });
      expect(validator.getConfig().maxRequestSize).toBe(1024 * 1024);
    });
  });

  describe('createStreamingResponseBuilder', () => {
    it('should create builder with default config', () => {
      const builder = createStreamingResponseBuilder();
      expect(builder).toBeInstanceOf(StreamingResponseBuilder);
    });

    it('should create builder with custom config', () => {
      const builder = createStreamingResponseBuilder({
        cursorBatchTargetSize: 1024,
      });
      expect(builder).toBeInstanceOf(StreamingResponseBuilder);
    });
  });
});

// ============================================================================
// Utility Functions Tests
// ============================================================================

describe('Utility Functions', () => {
  describe('isValidRequestSize', () => {
    it('should return true for valid sizes', () => {
      expect(isValidRequestSize(100)).toBe(true);
      expect(isValidRequestSize(1024 * 1024)).toBe(true);
      expect(isValidRequestSize(MONGODB_MAX_DOCUMENT_SIZE)).toBe(true);
    });

    it('should return false for invalid sizes', () => {
      expect(isValidRequestSize(0)).toBe(false);
      expect(isValidRequestSize(-1)).toBe(false);
      expect(isValidRequestSize(MONGODB_MAX_DOCUMENT_SIZE + 1)).toBe(false);
    });

    it('should accept custom max size', () => {
      expect(isValidRequestSize(500, 1000)).toBe(true);
      expect(isValidRequestSize(1500, 1000)).toBe(false);
    });
  });

  describe('isValidDocumentSize', () => {
    it('should return true for valid sizes', () => {
      expect(isValidDocumentSize(100)).toBe(true);
      expect(isValidDocumentSize(MONGODB_MAX_DOCUMENT_SIZE)).toBe(true);
    });

    it('should return false for invalid sizes', () => {
      expect(isValidDocumentSize(0)).toBe(false);
      expect(isValidDocumentSize(MONGODB_MAX_DOCUMENT_SIZE + 1)).toBe(false);
    });

    it('should accept custom max size', () => {
      expect(isValidDocumentSize(500, 1000)).toBe(true);
      expect(isValidDocumentSize(1500, 1000)).toBe(false);
    });
  });

  describe('formatBytes', () => {
    it('should format bytes correctly', () => {
      expect(formatBytes(0)).toBe('0 B');
      expect(formatBytes(100)).toBe('100 B');
      expect(formatBytes(1024)).toBe('1.00 KB');
      expect(formatBytes(1536)).toBe('1.50 KB');
      expect(formatBytes(1024 * 1024)).toBe('1.00 MB');
      expect(formatBytes(1.5 * 1024 * 1024)).toBe('1.50 MB');
      expect(formatBytes(1024 * 1024 * 1024)).toBe('1.00 GB');
      expect(formatBytes(16 * 1024 * 1024)).toBe('16.00 MB');
    });

    it('should handle large values', () => {
      expect(formatBytes(1024 * 1024 * 1024 * 1024)).toBe('1.00 TB');
    });
  });

  describe('buildSizeLimitErrorResponse', () => {
    it('should build valid error response', () => {
      const error = new SizeLimitError(
        'Test error',
        SizeLimitErrorCode.RequestTooLarge,
        'RequestTooLarge',
        20000000,
        16777216,
        'request'
      );

      const response = buildSizeLimitErrorResponse(1, 0, error);

      expect(response).toBeInstanceOf(Uint8Array);
      expect(response.length).toBeGreaterThan(0);

      const parsed = parseMessage(response);
      expect(parsed.type).toBe('OP_MSG');
    });
  });
});

// ============================================================================
// Wire Protocol Integration Tests
// ============================================================================

describe('Wire Protocol Integration', () => {
  describe('Message Parser Size Limits', () => {
    it('should reject wire message exceeding 48MB', () => {
      const oversized = MAX_WIRE_MESSAGE_SIZE + 1;
      const header = createHeader(oversized, 1, 0, OpCode.OP_MSG);

      expect(() => parseMessage(header)).toThrow(/exceeds maximum/i);
    });

    it('should accept wire message at maximum size boundary', () => {
      const header = createHeader(MAX_WIRE_MESSAGE_SIZE, 1, 0, OpCode.OP_MSG);
      const parsedHeader = parseMessageHeader(header);

      expect(parsedHeader.messageLength).toBe(MAX_WIRE_MESSAGE_SIZE);
    });

    it('should reject message one byte over maximum', () => {
      const oneOverMax = MAX_WIRE_MESSAGE_SIZE + 1;
      const header = createHeader(oneOverMax, 1, 0, OpCode.OP_MSG);

      expect(() => parseMessage(header)).toThrow(/exceeds maximum/i);
    });

    it('should accept message one byte under maximum', () => {
      const oneUnderMax = MAX_WIRE_MESSAGE_SIZE - 1;
      const header = createHeader(oneUnderMax, 1, 0, OpCode.OP_MSG);

      const parsed = parseMessageHeader(header);
      expect(parsed.messageLength).toBe(oneUnderMax);
    });

    it('should handle message at exact minimum size', () => {
      const header = createHeader(MIN_WIRE_MESSAGE_SIZE, 1, 0, OpCode.OP_MSG);

      const parsed = parseMessageHeader(header);
      expect(parsed.messageLength).toBe(MIN_WIRE_MESSAGE_SIZE);
    });
  });

  describe('Streaming Parser Size Limits', () => {
    it('should reject oversized message early (before full read)', () => {
      const parser = new StreamingMessageParser();
      const oversizedHeader = createHeader(100 * 1024 * 1024, 1, 0, OpCode.OP_MSG);

      const result = parser.feed(oversizedHeader);

      expect(result.state).toBe('error');
      expect(result.error?.message).toMatch(/exceeds maximum/i);
    });

    it('should not buffer full oversized message before rejecting', () => {
      const parser = new StreamingMessageParser(1024);
      const oversizedHeader = createHeader(50 * 1024 * 1024, 1, 0, OpCode.OP_MSG);

      const result = parser.feed(oversizedHeader);

      expect(result.state).toBe('error');
      expect(parser.getBufferedLength()).toBeLessThan(1024 * 1024);
    });

    it('should recover after rejecting oversized request', () => {
      const parser = new StreamingMessageParser();

      const oversizedHeader = createHeader(100 * 1024 * 1024, 1, 0, OpCode.OP_MSG);
      parser.feed(oversizedHeader);

      parser.reset();

      const doc = serializeDocument({ ping: 1, $db: 'admin' });
      const validMessage = createOpMsg(2, 0, 0, [{ type: 0, payload: doc }]);

      const result = parser.feed(validMessage);

      expect(result.state).toBe('complete');
      expect(result.message).toBeDefined();
    });
  });

  describe('Response Building', () => {
    it('should build response within size limits', () => {
      const response = buildSuccessResponse(1, 2, {
        cursor: {
          id: 0n,
          ns: 'test.collection',
          firstBatch: [{ _id: '1', name: 'test' }],
        },
      });

      expect(response.length).toBeLessThan(MAX_WIRE_MESSAGE_SIZE);
    });

    it('should handle response at maximum allowed size boundary', () => {
      const docs = Array.from({ length: 1000 }, (_, i) => ({
        _id: i.toString(),
        data: 'x'.repeat(1000),
      }));

      const response = buildCursorResponse(1, 2, 0n, 'test.coll', docs, 'firstBatch');

      expect(response.length).toBeLessThan(MAX_WIRE_MESSAGE_SIZE);
    });

    it('should format error response matching MongoDB protocol', () => {
      const error = buildErrorResponse(
        1,
        2,
        MongoErrorCode.InvalidLength,
        'Size 17825792 is larger than MaxDocumentSize 16777216'
      );

      const parsed = parseMessage(error);
      expect(parsed.type).toBe('OP_MSG');
    });
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('End-to-End Integration', () => {
  it('should validate and build streaming response for large batch', () => {
    const validator = createSizeLimitValidator({
      cursorBatchTargetSize: 1000,
    });
    const builder = new StreamingResponseBuilder(validator);

    const documents = Array(100).fill({ name: 'test', value: 123 });
    const sizes = Array(100).fill(100);

    const batchResult = validator.validateBatch(sizes);
    expect(batchResult.valid).toBe(true);

    const response = builder.buildResponse(
      1, 0,
      'testdb.users',
      documents,
      sizes
    );

    expect(response.hasMore).toBe(true);
    expect(response.cursorId).not.toBe(0n);
  });

  it('should reject oversized request and build error response', () => {
    const validator = new SizeLimitValidator();
    const oversizedRequest = MONGODB_MAX_DOCUMENT_SIZE + 1;

    const result = validator.validateRequest(oversizedRequest);
    expect(result.valid).toBe(false);
    expect(result.error).toBeDefined();

    const errorResponse = result.error!.toErrorResponse(1, 0);
    expect(errorResponse).toBeInstanceOf(Uint8Array);

    const parsed = parseMessage(errorResponse);
    expect(parsed.type).toBe('OP_MSG');
  });

  it('should handle complete cursor iteration', () => {
    const validator = createSizeLimitValidator({
      cursorBatchTargetSize: 300,
    });
    const builder = new StreamingResponseBuilder(validator);

    const documents = Array(20).fill({ x: 1 });
    const sizes = Array(20).fill(100);

    const firstResult = builder.buildResponse(
      1, 0,
      'testdb.users',
      documents,
      sizes
    );

    expect(firstResult.includedCount).toBeGreaterThan(0);
    expect(firstResult.includedCount).toBeLessThan(20);

    let processed = firstResult.includedCount;
    let cursorId = firstResult.cursorId;
    let iterations = 0;
    const maxIterations = 100;

    while (processed < 20 && iterations < maxIterations) {
      const moreResult = builder.buildGetMoreResponse(
        2, 0,
        cursorId,
        'testdb.users',
        documents,
        sizes,
        processed
      );

      processed += moreResult.includedCount;
      cursorId = moreResult.cursorId;
      iterations++;

      if (!moreResult.hasMore) {
        break;
      }
    }

    expect(processed).toBe(20);
    expect(cursorId).toBe(0n);
  });
});

// ============================================================================
// Edge Cases Tests
// ============================================================================

describe('Edge Cases', () => {
  it('should handle zero-length arrays', () => {
    const validator = new SizeLimitValidator();
    const result = validator.validateBatch([]);
    expect(result.valid).toBe(true);
  });

  it('should handle single very large document', () => {
    const validator = new SizeLimitValidator();
    const result = validator.validateBatch([MONGODB_MAX_DOCUMENT_SIZE + 1]);
    expect(result.valid).toBe(false);
    expect(result.oversizedDocuments).toEqual([0]);
  });

  it('should handle mixed size documents in batch', () => {
    const validator = new SizeLimitValidator();
    const sizes = [100, 5000, 200, 10000, 50];
    const result = validator.validateBatch(sizes);
    expect(result.valid).toBe(true);
    expect(result.totalSize).toBe(15350);
  });

  it('should handle configuration with all zeros', () => {
    const validator = new SizeLimitValidator({
      maxRequestSize: 0,
      maxResponseSize: 0,
      maxDocumentSize: 0,
      maxBatchCount: 0,
      maxBatchSize: 0,
    });

    const result = validator.validateRequest(1);
    expect(result.valid).toBe(false);
  });

  it('should handle very small limits', () => {
    const validator = new SizeLimitValidator({
      maxRequestSize: 10,
      maxDocumentSize: 10,
    });

    expect(validator.validateRequest(5).valid).toBe(true);
    expect(validator.validateRequest(15).valid).toBe(false);
    expect(validator.validateDocument(5).valid).toBe(true);
    expect(validator.validateDocument(15).valid).toBe(false);
  });

  it('should handle streaming response with empty document array', () => {
    const builder = new StreamingResponseBuilder();
    const result = builder.buildResponse(
      1, 0,
      'testdb.users',
      [],
      []
    );

    expect(result.hasMore).toBe(false);
    expect(result.cursorId).toBe(0n);
    expect(result.includedCount).toBe(0);
  });
});

// ============================================================================
// Performance Tests
// ============================================================================

describe('Performance', () => {
  it('should handle large valid batch efficiently', () => {
    const start = performance.now();

    const docs = Array.from({ length: 10000 }, (_, i) => ({
      _id: i.toString(),
      data: `value-${i}`,
    }));

    for (const doc of docs) {
      serializeDocument(doc);
    }

    const elapsed = performance.now() - start;

    expect(elapsed).toBeLessThan(1000);
  });

  it('should validate batch sizes efficiently', () => {
    const validator = new SizeLimitValidator();
    const sizes = createDocumentSizes(100000, 100);

    const start = performance.now();
    const result = validator.validateBatch(sizes);
    const elapsed = performance.now() - start;

    expect(result.valid).toBe(true);
    expect(elapsed).toBeLessThan(100);
  });
});
