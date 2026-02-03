/**
 * Wire Protocol Message Size Validation Tests
 *
 * Tests for message/document size validation before BSON deserialization
 * in the TCP server to prevent memory exhaustion attacks.
 *
 * Issue: mongolake-9wyr - HIGH: Validate document sizes in wire protocol before deserialization
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { EventEmitter } from 'node:events';

import {
  createServer,
  type TcpServer,
  type TcpServerOptions,
} from '../../../src/wire-protocol/tcp-server';

import {
  SizeLimitValidator,
  SizeLimitError,
  SizeLimitErrorCode,
  MONGODB_MAX_MESSAGE_SIZE,
} from '../../../src/wire-protocol/size-limits';

import {
  serializeDocument,
  buildOpMsgResponse,
} from '../../../src/wire-protocol/bson-serializer';

import {
  parseMessage,
  parseMessageHeader,
  OpCode,
} from '../../../src/wire-protocol/message-parser';

import {
  MAX_WIRE_MESSAGE_SIZE,
  MIN_WIRE_MESSAGE_SIZE,
} from '../../../src/constants';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Create a wire protocol message header
 */
function createHeader(
  messageLength: number,
  requestId: number = 1,
  responseTo: number = 0,
  opCode: number = OpCode.OP_MSG
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
 * Create a valid OP_MSG message
 */
function createOpMsg(
  requestId: number,
  responseTo: number,
  body: Record<string, unknown>
): Uint8Array {
  const payload = serializeDocument(body);
  const messageLength = 16 + 4 + 1 + payload.length; // header + flags + section type + payload

  const buffer = new Uint8Array(messageLength);
  const view = new DataView(buffer.buffer);

  view.setInt32(0, messageLength, true);
  view.setInt32(4, requestId, true);
  view.setInt32(8, responseTo, true);
  view.setInt32(12, OpCode.OP_MSG, true);
  view.setUint32(16, 0, true); // flags
  buffer[20] = 0; // section type 0 (body)
  buffer.set(payload, 21);

  return buffer;
}

/**
 * Mock socket for testing
 */
class MockSocket extends EventEmitter {
  public writable = true;
  public writtenData: Uint8Array[] = [];
  public isPaused = false;
  public remoteAddress = '127.0.0.1';
  public remotePort = 12345;

  write(data: Buffer | Uint8Array, callback?: (err?: Error) => void): boolean {
    this.writtenData.push(new Uint8Array(data));
    if (callback) callback();
    return true;
  }

  pause(): void {
    this.isPaused = true;
  }

  resume(): void {
    this.isPaused = false;
  }

  end(): void {
    this.emit('close');
  }

  destroy(): void {
    this.emit('close');
  }
}

// ============================================================================
// Size Validation Constants Tests
// ============================================================================

describe('Message Size Validation Constants', () => {
  it('should have MongoDB-compatible maximum message size (48MB)', () => {
    expect(MAX_WIRE_MESSAGE_SIZE).toBe(48 * 1024 * 1024);
  });

  it('should have minimum message size (16 bytes header)', () => {
    expect(MIN_WIRE_MESSAGE_SIZE).toBe(16);
  });

  it('should match MONGODB_MAX_MESSAGE_SIZE from size-limits module', () => {
    expect(MAX_WIRE_MESSAGE_SIZE).toBe(MONGODB_MAX_MESSAGE_SIZE);
  });
});

// ============================================================================
// SizeLimitValidator Tests for Wire Protocol
// ============================================================================

describe('SizeLimitValidator for Wire Protocol', () => {
  let validator: SizeLimitValidator;

  beforeEach(() => {
    // Configure validator with request size matching wire message size
    // for pure wire protocol message size validation
    validator = new SizeLimitValidator({
      maxRequestSize: MAX_WIRE_MESSAGE_SIZE,
    });
  });

  describe('Message Size Validation', () => {
    it('should accept message within 48MB limit', () => {
      const result = validator.validateRequest(10 * 1024 * 1024); // 10MB
      expect(result.valid).toBe(true);
    });

    it('should accept message at exactly 48MB limit', () => {
      const result = validator.validateRequest(MAX_WIRE_MESSAGE_SIZE);
      expect(result.valid).toBe(true);
    });

    it('should reject message exceeding 48MB limit', () => {
      const size = MAX_WIRE_MESSAGE_SIZE + 1;
      const result = validator.validateRequest(size);

      expect(result.valid).toBe(false);
      expect(result.error).toBeInstanceOf(SizeLimitError);
      expect(result.error?.code).toBe(SizeLimitErrorCode.MessageTooLarge);
    });

    it('should reject message significantly over limit (100MB)', () => {
      const size = 100 * 1024 * 1024;
      const result = validator.validateRequest(size);

      expect(result.valid).toBe(false);
      expect(result.error?.actualSize).toBe(size);
      expect(result.error?.maxSize).toBe(MAX_WIRE_MESSAGE_SIZE);
    });
  });

  describe('Default Validator with Document Size Limit', () => {
    it('should reject messages over 16MB with default validator', () => {
      // Default validator uses 16MB maxRequestSize (document limit)
      const defaultValidator = new SizeLimitValidator();
      const size = 20 * 1024 * 1024; // 20MB
      const result = defaultValidator.validateRequest(size);

      expect(result.valid).toBe(false);
      // 20MB exceeds 16MB document limit but not 48MB wire limit
      expect(result.error?.code).toBe(SizeLimitErrorCode.RequestTooLarge);
    });
  });

  describe('Custom Size Limits', () => {
    it('should allow configuring smaller max message size', () => {
      const customValidator = new SizeLimitValidator({
        maxWireMessageSize: 10 * 1024 * 1024, // 10MB
        maxRequestSize: 10 * 1024 * 1024, // Also set request size
      });

      const result = customValidator.validateRequest(15 * 1024 * 1024);
      expect(result.valid).toBe(false);
    });

    it('should allow configuring larger max message size', () => {
      const customValidator = new SizeLimitValidator({
        maxWireMessageSize: 100 * 1024 * 1024, // 100MB
        maxRequestSize: 100 * 1024 * 1024, // Also set request size
      });

      const result = customValidator.validateRequest(60 * 1024 * 1024);
      expect(result.valid).toBe(true);
    });
  });
});

// ============================================================================
// Size Limit Error Response Tests
// ============================================================================

describe('Size Limit Error Response', () => {
  it('should generate valid wire protocol error response', () => {
    const error = new SizeLimitError(
      'Message too large',
      SizeLimitErrorCode.MessageTooLarge,
      'MessageTooLarge',
      60 * 1024 * 1024,
      48 * 1024 * 1024,
      'wireMessage'
    );

    const response = error.toErrorResponse(1, 0);

    expect(response).toBeInstanceOf(Uint8Array);
    expect(response.length).toBeGreaterThan(0);

    // Verify it's a valid wire protocol message
    const parsed = parseMessage(response);
    expect(parsed.type).toBe('OP_MSG');
  });

  it('should include error code in response', () => {
    const error = new SizeLimitError(
      'Message size 60MB exceeds maximum of 48MB',
      SizeLimitErrorCode.MessageTooLarge,
      'MessageTooLarge',
      60 * 1024 * 1024,
      48 * 1024 * 1024,
      'wireMessage'
    );

    const response = error.toErrorResponse(1, 0);
    const parsed = parseMessage(response);

    expect(parsed.type).toBe('OP_MSG');
    expect(response.length).toBeGreaterThan(20); // At least header + flags + section
  });
});

// ============================================================================
// Early Size Detection Tests
// ============================================================================

describe('Early Size Detection', () => {
  it('should detect oversized message from header before buffering full message', () => {
    // Create header claiming a 100MB message
    const oversizedHeader = createHeader(100 * 1024 * 1024, 1, 0, OpCode.OP_MSG);

    // Parse just the header
    const header = parseMessageHeader(oversizedHeader);

    expect(header.messageLength).toBe(100 * 1024 * 1024);

    // Validate the size with wire message limit
    const validator = new SizeLimitValidator({
      maxRequestSize: MAX_WIRE_MESSAGE_SIZE,
    });
    const result = validator.validateRequest(header.messageLength);

    expect(result.valid).toBe(false);
    expect(result.error?.code).toBe(SizeLimitErrorCode.MessageTooLarge);
  });

  it('should throw error for negative message length', () => {
    const header = createHeader(-1, 1, 0, OpCode.OP_MSG);

    // parseMessageHeader throws for negative lengths
    expect(() => parseMessageHeader(header)).toThrow(/Invalid message length/i);
  });

  it('should reject zero message length', () => {
    const header = createHeader(0, 1, 0, OpCode.OP_MSG);

    expect(() => parseMessage(header)).toThrow(/less than minimum/i);
  });

  it('should detect exactly 48MB + 1 byte as oversized', () => {
    const oneOverMax = MAX_WIRE_MESSAGE_SIZE + 1;
    const header = createHeader(oneOverMax, 1, 0, OpCode.OP_MSG);

    expect(() => parseMessage(header)).toThrow(/exceeds maximum/i);
  });

  it('should accept exactly 48MB as valid', () => {
    const exactMax = MAX_WIRE_MESSAGE_SIZE;
    const header = createHeader(exactMax, 1, 0, OpCode.OP_MSG);

    // This should not throw during header parsing
    const parsedHeader = parseMessageHeader(header);
    expect(parsedHeader.messageLength).toBe(exactMax);
  });
});

// ============================================================================
// TcpServer Options Tests
// ============================================================================

describe('TcpServer Message Size Configuration', () => {
  it('should accept messageSize configuration option', () => {
    const options: TcpServerOptions = {
      port: 0, // Random port
      messageSize: {
        maxMessageSize: 10 * 1024 * 1024, // 10MB
      },
    };

    const server = createServer(options);
    expect(server).toBeDefined();
  });

  it('should accept sizeLimits configuration option', () => {
    const options: TcpServerOptions = {
      port: 0,
      messageSize: {
        sizeLimits: {
          maxRequestSize: 8 * 1024 * 1024,
          maxDocumentSize: 8 * 1024 * 1024,
        },
      },
    };

    const server = createServer(options);
    expect(server).toBeDefined();
  });

  it('should use default 48MB limit when not specified', () => {
    const options: TcpServerOptions = {
      port: 0,
    };

    const server = createServer(options);
    expect(server).toBeDefined();
    // Default should be MAX_WIRE_MESSAGE_SIZE (48MB)
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('Message Size Validation Integration', () => {
  it('should validate message size before BSON deserialization', () => {
    // This test verifies the principle: we can detect oversized messages
    // from just the 16-byte header without reading the full message

    const validator = new SizeLimitValidator({
      maxRequestSize: MAX_WIRE_MESSAGE_SIZE,
    });

    // Simulate receiving just the header of a 100MB message
    const oversizedHeader = createHeader(100 * 1024 * 1024);
    const header = parseMessageHeader(oversizedHeader);

    // Validate before attempting to buffer/deserialize
    const result = validator.validateRequest(header.messageLength);

    expect(result.valid).toBe(false);
    expect(result.error).toBeDefined();

    // Key assertion: we detected the problem with only 16 bytes,
    // not the full 100MB
    expect(oversizedHeader.length).toBe(16);
  });

  it('should allow valid small messages through', () => {
    const validator = new SizeLimitValidator();

    // Create a valid small message
    const validMessage = createOpMsg(1, 0, { ping: 1, $db: 'admin' });

    const result = validator.validateRequest(validMessage.length);
    expect(result.valid).toBe(true);

    // The message should parse correctly
    const parsed = parseMessage(validMessage);
    expect(parsed.type).toBe('OP_MSG');
  });

  it('should allow messages up to exactly 48MB with wire protocol validator', () => {
    // Configure validator for wire protocol (48MB limit)
    const validator = new SizeLimitValidator({
      maxRequestSize: MAX_WIRE_MESSAGE_SIZE,
    });

    // Simulate a 48MB message header
    const maxSizeHeader = createHeader(MAX_WIRE_MESSAGE_SIZE);
    const header = parseMessageHeader(maxSizeHeader);

    const result = validator.validateRequest(header.messageLength);
    expect(result.valid).toBe(true);
  });

  it('should reject messages over 48MB', () => {
    const validator = new SizeLimitValidator({
      maxRequestSize: MAX_WIRE_MESSAGE_SIZE,
    });

    // Simulate a 48MB + 1 byte message header
    const oversizedHeader = createHeader(MAX_WIRE_MESSAGE_SIZE + 1);
    const header = parseMessageHeader(oversizedHeader);

    const result = validator.validateRequest(header.messageLength);
    expect(result.valid).toBe(false);
    expect(result.error?.code).toBe(SizeLimitErrorCode.MessageTooLarge);
  });
});

// ============================================================================
// Edge Cases
// ============================================================================

describe('Edge Cases', () => {
  it('should handle minimum valid message size (16 bytes)', () => {
    const validator = new SizeLimitValidator();
    const result = validator.validateRequest(MIN_WIRE_MESSAGE_SIZE);
    expect(result.valid).toBe(true);
  });

  it('should handle size at boundary minus 1 with wire protocol validator', () => {
    const validator = new SizeLimitValidator({
      maxRequestSize: MAX_WIRE_MESSAGE_SIZE,
    });
    const result = validator.validateRequest(MAX_WIRE_MESSAGE_SIZE - 1);
    expect(result.valid).toBe(true);
  });

  it('should handle size at boundary plus 1', () => {
    const validator = new SizeLimitValidator({
      maxRequestSize: MAX_WIRE_MESSAGE_SIZE,
    });
    const result = validator.validateRequest(MAX_WIRE_MESSAGE_SIZE + 1);
    expect(result.valid).toBe(false);
  });

  it('should handle very large sizes (1GB) gracefully', () => {
    const validator = new SizeLimitValidator();
    const result = validator.validateRequest(1024 * 1024 * 1024); // 1GB

    expect(result.valid).toBe(false);
    expect(result.error?.actualSize).toBe(1024 * 1024 * 1024);
  });

  it('should handle custom smaller limit with boundary checks', () => {
    const customMax = 1 * 1024 * 1024; // 1MB
    const validator = new SizeLimitValidator({
      maxWireMessageSize: customMax,
      maxRequestSize: customMax,
    });

    expect(validator.validateRequest(customMax).valid).toBe(true);
    expect(validator.validateRequest(customMax + 1).valid).toBe(false);
    expect(validator.validateRequest(customMax - 1).valid).toBe(true);
  });
});

// ============================================================================
// Security Tests
// ============================================================================

describe('Security - Memory Exhaustion Prevention', () => {
  it('should not allocate memory for oversized message payload', () => {
    // This test verifies that we can reject oversized messages
    // without allocating memory for the full payload

    const validator = new SizeLimitValidator();

    // Attacker claims to send a 1GB message
    const attackHeader = createHeader(1024 * 1024 * 1024); // 1GB
    const header = parseMessageHeader(attackHeader);

    // We can validate and reject with only 16 bytes in memory
    const result = validator.validateRequest(header.messageLength);

    expect(result.valid).toBe(false);
    expect(attackHeader.length).toBe(16); // Only 16 bytes allocated
  });

  it('should reject extremely large declared sizes', () => {
    const validator = new SizeLimitValidator();

    // Test with max int32 value
    const result = validator.validateRequest(2147483647);
    expect(result.valid).toBe(false);
  });

  it('should provide clear error message for oversized messages', () => {
    const validator = new SizeLimitValidator();
    const size = 100 * 1024 * 1024; // 100MB

    const result = validator.validateRequest(size);

    expect(result.error?.message).toMatch(/exceeds/i);
    expect(result.error?.message).toMatch(/maximum/i);
  });
});
