/**
 * MongoDB Wire Protocol Message Parser
 *
 * Parses MongoDB wire protocol messages, specifically:
 * - Message headers (16 bytes: length, requestId, responseTo, opCode)
 * - OP_MSG messages (opCode 2013) - the modern MongoDB message format
 * - Command document extraction
 * - Checksums and flags handling
 * - Streaming parser for handling large messages incrementally
 *
 * Wire Protocol Reference: https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/
 */

import { MAX_WIRE_MESSAGE_SIZE, MIN_WIRE_MESSAGE_SIZE } from '../constants.js';

// ============================================================================
// Constants
// ============================================================================

/** MongoDB wire protocol operation codes */
export const OpCode = {
  OP_REPLY: 1, // Deprecated - Reply to a client request
  OP_UPDATE: 2001, // Deprecated - Update document
  OP_INSERT: 2002, // Deprecated - Insert new document
  OP_QUERY: 2004, // Deprecated - Query a collection
  OP_GET_MORE: 2005, // Deprecated - Get more data from a query
  OP_DELETE: 2006, // Deprecated - Delete documents
  OP_KILL_CURSORS: 2007, // Deprecated - Notify database client is done with cursor
  OP_COMPRESSED: 2012, // Compressed message
  OP_MSG: 2013, // Standard message format since MongoDB 3.6
} as const;

export type OpCode = (typeof OpCode)[keyof typeof OpCode];

/** OP_MSG flag bits */
export const OpMsgFlags = {
  checksumPresent: 1 << 0, // Bit 0: Message includes CRC-32C checksum
  moreToCome: 1 << 1, // Bit 1: More messages coming, don't respond yet
  exhaustAllowed: 1 << 16, // Bit 16: Client is prepared for exhaust streaming
} as const;

export type OpMsgFlags = (typeof OpMsgFlags)[keyof typeof OpMsgFlags];

/** Maximum message size - imported from constants */
const MAX_MESSAGE_SIZE = MAX_WIRE_MESSAGE_SIZE;

/** Minimum message size (header only) - imported from constants */
const MIN_MESSAGE_SIZE = MIN_WIRE_MESSAGE_SIZE;

/** Wire protocol header size in bytes */
const HEADER_SIZE = 16;

/** OP_MSG flags field size in bytes */
const FLAGS_SIZE = 4;

/** Reserved flag mask - bits 2-15 and 17-31 must be 0 */
const RESERVED_FLAG_MASK = 0b11111111111111101111111111111100;

/** Reusable TextDecoder instance for better performance */
const textDecoder = new TextDecoder();

// ============================================================================
// Types
// ============================================================================

/** MongoDB wire protocol message header */
export interface MessageHeader {
  messageLength: number;
  requestId: number;
  responseTo: number;
  opCode: number;
}

/** OP_MSG section - either body (type 0) or document sequence (type 1) */
export interface OpMsgSection {
  type: 0 | 1;
  payload: Uint8Array;
  identifier?: string; // Only for type 1 (document sequence)
  documents?: Document[]; // Parsed documents (populated on demand)
}

/** Parsed OP_MSG message */
export interface OpMsgMessage {
  header: MessageHeader;
  flags: number;
  sections: OpMsgSection[];
  checksum?: number;
}

/** Parsed OP_QUERY message */
export interface OpQueryMessage {
  header: MessageHeader;
  flags: number;
  fullCollectionName: string;
  numberToSkip: number;
  numberToReturn: number;
  query: Document;
  returnFieldsSelector?: Document;
}

/** Generic parsed message (discriminated union) */
export type ParsedMessage =
  | { header: MessageHeader; type: 'OP_MSG'; message: OpMsgMessage }
  | { header: MessageHeader; type: 'OP_QUERY'; opQuery: OpQueryMessage }
  | { header: MessageHeader; type: 'UNKNOWN'; rawBody: Uint8Array };

/** Generic document type */
export interface Document {
  [key: string]: unknown;
}

/** Extracted command from OP_MSG */
export interface ExtractedCommand {
  name: string;
  collection?: string;
  database: string;
  cursorId?: number;
  documents?: Document[];
  options?: {
    ordered?: boolean;
    [key: string]: unknown;
  };
  body: Document;
}

/** Streaming parser state */
export type StreamingParserState =
  | 'awaiting_header'
  | 'awaiting_body'
  | 'complete'
  | 'error';

/** Result from streaming parser feed operation */
export interface StreamingParserResult {
  state: StreamingParserState;
  message?: ParsedMessage;
  bytesConsumed: number;
  error?: Error;
}

// ============================================================================
// BSON Parser (minimal implementation for wire protocol)
// ============================================================================

/** BSON element type codes */
const BsonType = {
  DOUBLE: 0x01,
  STRING: 0x02,
  DOCUMENT: 0x03,
  ARRAY: 0x04,
  BINARY: 0x05,
  UNDEFINED: 0x06, // Deprecated
  OBJECT_ID: 0x07,
  BOOLEAN: 0x08,
  DATE: 0x09,
  NULL: 0x0a,
  REGEX: 0x0b,
  DBPOINTER: 0x0c, // Deprecated
  JAVASCRIPT: 0x0d,
  SYMBOL: 0x0e, // Deprecated
  CODE_W_SCOPE: 0x0f,
  INT32: 0x10,
  TIMESTAMP: 0x11,
  INT64: 0x12,
  DECIMAL128: 0x13,
  MIN_KEY: 0xff,
  MAX_KEY: 0x7f,
} as const;

/** Minimum valid BSON document size (4 bytes size + 1 byte terminator) */
const MIN_BSON_DOC_SIZE = 5;

/**
 * Read a C-string (null-terminated) from buffer at given position
 *
 * @returns The decoded string and the number of bytes consumed (including null terminator)
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
  return { value, bytesRead: end - offset + 1 }; // +1 for null terminator
}

/**
 * Parse a BSON document from a buffer
 *
 * @param buffer - Buffer containing the BSON document
 * @param offset - Byte offset into the buffer (default 0)
 * @returns Parsed document and number of bytes consumed
 */
function parseBsonDocument(
  buffer: Uint8Array,
  offset: number = 0
): { doc: Document; bytesRead: number } {
  const view = new DataView(buffer.buffer, buffer.byteOffset + offset);

  // Read document size
  const docSize = view.getInt32(0, true);

  if (docSize < MIN_BSON_DOC_SIZE) {
    throw new Error('Invalid BSON document: size too small');
  }

  if (offset + docSize > buffer.length) {
    throw new Error('Invalid BSON document: size exceeds buffer');
  }

  // Check for terminator
  if (buffer[offset + docSize - 1] !== 0x00) {
    throw new Error('Invalid BSON document: missing terminator');
  }

  const doc: Document = {};
  let pos = 4; // Skip size field

  while (pos < docSize - 1) {
    // -1 for terminator
    const elementType = buffer[offset + pos];
    pos++;

    if (elementType === 0x00) {
      break; // End of document
    }

    // Read element name (C-string)
    const { value: name, bytesRead: nameBytes } = readCString(
      buffer,
      offset + pos,
      docSize - pos
    );
    pos += nameBytes;

    // Read value based on type
    const valueView = new DataView(buffer.buffer, buffer.byteOffset + offset + pos);

    switch (elementType) {
      case BsonType.DOUBLE: {
        doc[name] = valueView.getFloat64(0, true);
        pos += 8;
        break;
      }

      case BsonType.STRING: {
        const strLen = valueView.getInt32(0, true);
        if (strLen < 1) {
          throw new Error('Invalid BSON string: length must be at least 1');
        }
        pos += 4;
        // Validate that string data fits within document and doesn't eat terminator
        if (offset + pos + strLen > offset + docSize - 1) {
          throw new Error(
            'Invalid BSON string: length exceeds document bounds or eats document terminator'
          );
        }
        // Validate null terminator
        if (buffer[offset + pos + strLen - 1] !== 0x00) {
          throw new Error('Invalid BSON string: not null-terminated');
        }
        doc[name] = textDecoder.decode(
          buffer.subarray(offset + pos, offset + pos + strLen - 1)
        ); // -1 to exclude null
        pos += strLen;
        break;
      }

      case BsonType.DOCUMENT: {
        const nested = parseBsonDocument(buffer, offset + pos);
        doc[name] = nested.doc;
        pos += nested.bytesRead;
        break;
      }

      case BsonType.ARRAY: {
        const nested = parseBsonDocument(buffer, offset + pos);
        // Convert document to array
        const arr: unknown[] = [];
        const keys = Object.keys(nested.doc)
          .map(Number)
          .sort((a, b) => a - b);
        for (const key of keys) {
          arr.push(nested.doc[key.toString()]);
        }
        doc[name] = arr;
        pos += nested.bytesRead;
        break;
      }

      case BsonType.BINARY: {
        const binLen = valueView.getInt32(0, true);
        pos += 4;
        // Skip subtype byte (we don't currently use it for anything)
        pos += 1;
        doc[name] = buffer.slice(offset + pos, offset + pos + binLen);
        pos += binLen;
        break;
      }

      case BsonType.OBJECT_ID: {
        doc[name] = Array.from(buffer.subarray(offset + pos, offset + pos + 12))
          .map((b) => b.toString(16).padStart(2, '0'))
          .join('');
        pos += 12;
        break;
      }

      case BsonType.BOOLEAN: {
        doc[name] = buffer[offset + pos] !== 0x00;
        pos += 1;
        break;
      }

      case BsonType.DATE: {
        const timestamp = Number(valueView.getBigInt64(0, true));
        doc[name] = new Date(timestamp);
        pos += 8;
        break;
      }

      case BsonType.NULL: {
        doc[name] = null;
        break;
      }

      case BsonType.REGEX: {
        // Read pattern
        const { value: pattern, bytesRead: patternBytes } = readCString(
          buffer,
          offset + pos,
          docSize - pos
        );
        pos += patternBytes;
        // Read options
        const { value: options, bytesRead: optionsBytes } = readCString(
          buffer,
          offset + pos,
          docSize - pos
        );
        pos += optionsBytes;
        doc[name] = new RegExp(pattern, options);
        break;
      }

      case BsonType.INT32: {
        doc[name] = valueView.getInt32(0, true);
        pos += 4;
        break;
      }

      case BsonType.TIMESTAMP:
      case BsonType.INT64: {
        doc[name] = Number(valueView.getBigInt64(0, true));
        pos += 8;
        break;
      }

      case BsonType.DECIMAL128: {
        // Store as raw bytes for now
        doc[name] = buffer.slice(offset + pos, offset + pos + 16);
        pos += 16;
        break;
      }

      case BsonType.JAVASCRIPT: {
        const jsLen = valueView.getInt32(0, true);
        if (jsLen < 1) {
          throw new Error('Invalid BSON JavaScript: length must be at least 1');
        }
        pos += 4;
        // Validate that JavaScript data fits within document and doesn't eat terminator
        if (offset + pos + jsLen > offset + docSize - 1) {
          throw new Error(
            'Invalid BSON JavaScript: length exceeds document bounds or eats document terminator'
          );
        }
        // Validate null terminator
        if (buffer[offset + pos + jsLen - 1] !== 0x00) {
          throw new Error('Invalid BSON JavaScript: not null-terminated');
        }
        doc[name] = textDecoder.decode(
          buffer.subarray(offset + pos, offset + pos + jsLen - 1)
        );
        pos += jsLen;
        break;
      }

      default:
        throw new Error(`Unsupported BSON type: 0x${elementType!.toString(16)}`);
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

// ============================================================================
// Message Parsing Functions
// ============================================================================

/**
 * Parse a MongoDB wire protocol message header
 *
 * @param buffer - Buffer containing at least 16 bytes
 * @param offset - Optional offset into buffer (default 0)
 * @returns Parsed message header
 */
export function parseMessageHeader(
  buffer: Uint8Array,
  offset: number = 0
): MessageHeader {
  if (buffer.length - offset < HEADER_SIZE) {
    throw new Error(
      `Buffer too small for message header: requires ${HEADER_SIZE} bytes`
    );
  }

  const view = new DataView(buffer.buffer, buffer.byteOffset + offset);

  const messageLength = view.getInt32(0, true);
  const requestId = view.getInt32(4, true);
  const responseTo = view.getInt32(8, true);
  const opCode = view.getInt32(12, true);

  // Validate message length
  if (messageLength < 0) {
    throw new Error(`Invalid message length: ${messageLength}`);
  }

  return {
    messageLength,
    requestId: requestId >>> 0, // Convert to unsigned
    responseTo,
    opCode,
  };
}

/** Size of checksum field in bytes */
const CHECKSUM_SIZE = 4;

/** OP_MSG section type: Body (single BSON document) */
const SECTION_TYPE_BODY = 0;

/** OP_MSG section type: Document sequence */
const SECTION_TYPE_DOC_SEQUENCE = 1;

/**
 * Parse an OP_MSG message
 *
 * @param buffer - Complete message buffer including header
 * @returns Parsed OP_MSG
 */
export function parseOpMsg(buffer: Uint8Array): OpMsgMessage {
  const header = parseMessageHeader(buffer);

  if (header.opCode !== OpCode.OP_MSG) {
    throw new Error(
      `Invalid opCode for OP_MSG: expected ${OpCode.OP_MSG}, got ${header.opCode}`
    );
  }

  if (buffer.length < header.messageLength) {
    throw new Error('Incomplete message: buffer shorter than declared length');
  }

  const view = new DataView(buffer.buffer, buffer.byteOffset);

  // Read flags (4 bytes after header)
  const flags = view.getUint32(HEADER_SIZE, true);

  // Check for reserved flag bits
  if (flags & RESERVED_FLAG_MASK) {
    throw new Error('Invalid OP_MSG: reserved flag bits are set');
  }

  // Check for invalid flag combination
  if (
    (flags & OpMsgFlags.exhaustAllowed) !== 0 &&
    (flags & OpMsgFlags.moreToCome) !== 0
  ) {
    throw new Error(
      'Invalid OP_MSG: exhaustAllowed and moreToCome cannot be used together'
    );
  }

  const checksumPresent = (flags & OpMsgFlags.checksumPresent) !== 0;

  // Calculate end of sections
  const messageEnd = checksumPresent
    ? header.messageLength - CHECKSUM_SIZE
    : header.messageLength;

  // Parse sections
  const sections: OpMsgSection[] = [];
  let pos = HEADER_SIZE + FLAGS_SIZE; // After header + flags

  while (pos < messageEnd) {
    const sectionType = buffer[pos];
    pos++;

    if (sectionType === SECTION_TYPE_BODY) {
      // Kind 0: Body - single BSON document
      const docView = new DataView(buffer.buffer, buffer.byteOffset + pos);
      const docSize = docView.getInt32(0, true);

      if (pos + docSize > messageEnd) {
        throw new Error('Incomplete BSON document in section');
      }

      // Validate BSON document
      try {
        parseBsonDocument(buffer, pos);
      } catch (e) {
        throw new Error(
          `Invalid BSON document: ${e instanceof Error ? e.message : 'unknown error'}`
        );
      }

      sections.push({
        type: SECTION_TYPE_BODY,
        payload: buffer.slice(pos, pos + docSize),
      });
      pos += docSize;
    } else if (sectionType === SECTION_TYPE_DOC_SEQUENCE) {
      // Kind 1: Document Sequence
      const seqView = new DataView(buffer.buffer, buffer.byteOffset + pos);
      const sectionSize = seqView.getInt32(0, true);

      if (pos + sectionSize > messageEnd) {
        throw new Error('Incomplete document sequence section');
      }

      // Read identifier (C-string)
      const { value: identifier, bytesRead: idBytes } = readCString(
        buffer,
        pos + 4,
        sectionSize - 4
      );

      // Documents follow the identifier
      const documentsStart = pos + 4 + idBytes;
      const documentsEnd = pos + sectionSize;

      sections.push({
        type: SECTION_TYPE_DOC_SEQUENCE,
        payload: buffer.slice(documentsStart, documentsEnd),
        identifier,
      });

      pos += sectionSize;
    } else {
      throw new Error(`Unknown section type: ${sectionType}`);
    }
  }

  // Read checksum if present
  let checksum: number | undefined;
  if (checksumPresent) {
    checksum = view.getUint32(messageEnd, true);
  }

  return {
    header,
    flags,
    sections,
    checksum,
  };
}

/**
 * Parse an OP_QUERY message (legacy format, but still used by some clients)
 *
 * @param buffer - Complete message buffer including header
 * @returns Parsed OP_QUERY
 */
export function parseOpQuery(buffer: Uint8Array): OpQueryMessage {
  const header = parseMessageHeader(buffer);

  if (header.opCode !== OpCode.OP_QUERY) {
    throw new Error(
      `Invalid opCode for OP_QUERY: expected ${OpCode.OP_QUERY}, got ${header.opCode}`
    );
  }

  if (buffer.length < header.messageLength) {
    throw new Error('Incomplete message: buffer shorter than declared length');
  }

  const view = new DataView(buffer.buffer, buffer.byteOffset);

  // Read flags (4 bytes after header)
  const flags = view.getInt32(HEADER_SIZE, true);

  // Read fullCollectionName (C-string after flags)
  let pos = HEADER_SIZE + FLAGS_SIZE;
  const { value: fullCollectionName, bytesRead: nameBytes } = readCString(
    buffer,
    pos,
    header.messageLength - pos
  );
  pos += nameBytes;

  // Read numberToSkip and numberToReturn
  const queryView = new DataView(buffer.buffer, buffer.byteOffset + pos);
  const numberToSkip = queryView.getInt32(0, true);
  const numberToReturn = queryView.getInt32(4, true);
  pos += 8;

  // Parse query document
  const { doc: query, bytesRead: queryBytes } = parseBsonDocument(buffer, pos);
  pos += queryBytes;

  // Parse optional returnFieldsSelector
  let returnFieldsSelector: Document | undefined;
  if (pos < header.messageLength) {
    const { doc } = parseBsonDocument(buffer, pos);
    returnFieldsSelector = doc;
  }

  return {
    header,
    flags,
    fullCollectionName,
    numberToSkip,
    numberToReturn,
    query,
    returnFieldsSelector,
  };
}

/**
 * Parse any MongoDB wire protocol message
 *
 * @param buffer - Complete message buffer
 * @returns Parsed message with type information
 */
export function parseMessage(buffer: Uint8Array): ParsedMessage {
  if (buffer.length < HEADER_SIZE) {
    throw new Error(
      `incomplete message: buffer must be at least ${HEADER_SIZE} bytes`
    );
  }

  const header = parseMessageHeader(buffer);

  // Validate message length
  if (header.messageLength < MIN_MESSAGE_SIZE) {
    throw new Error(
      `Invalid message length: ${header.messageLength} is less than minimum ${MIN_MESSAGE_SIZE}`
    );
  }

  if (header.messageLength > MAX_MESSAGE_SIZE) {
    throw new Error(
      `Message size exceeds maximum: ${header.messageLength} > ${MAX_MESSAGE_SIZE}`
    );
  }

  if (buffer.length < header.messageLength) {
    throw new Error(
      `Incomplete/truncated message: got ${buffer.length} bytes, expected ${header.messageLength}`
    );
  }

  if (header.opCode === OpCode.OP_MSG) {
    const message = parseOpMsg(buffer);
    return {
      header,
      type: 'OP_MSG',
      message,
    };
  }

  if (header.opCode === OpCode.OP_QUERY) {
    const opQuery = parseOpQuery(buffer);
    return {
      header,
      type: 'OP_QUERY',
      opQuery,
    };
  }

  // Unknown/unsupported opCode
  return {
    header,
    type: 'UNKNOWN',
    rawBody: buffer.slice(HEADER_SIZE, header.messageLength),
  };
}

// ============================================================================
// Command Extraction
// ============================================================================

/** Known MongoDB command names that take a collection as their value */
const COLLECTION_COMMANDS = new Set([
  'find',
  'insert',
  'update',
  'delete',
  'aggregate',
  'count',
  'distinct',
  'findAndModify',
  'mapReduce',
  'createIndexes',
  'dropIndexes',
  'drop',
  'create',
  'collMod',
]);

/** Admin commands (no collection) - exported for use by command handlers */
export const ADMIN_COMMANDS = new Set([
  'ping',
  'hello',
  'isMaster',
  'ismaster',
  'buildInfo',
  'serverStatus',
  'listDatabases',
  'listCollections',
  'getParameter',
  'setParameter',
  'currentOp',
  'killOp',
  'replSetGetStatus',
]);

/**
 * Extract the command from an OP_MSG message
 *
 * @param msg - Parsed OP_MSG message
 * @returns Extracted command details
 */
export function extractCommand(msg: OpMsgMessage): ExtractedCommand {
  // Find the body section (type 0)
  const bodySection = msg.sections.find((s) => s.type === 0);

  if (!bodySection) {
    throw new Error('OP_MSG must have at least one body section (type 0)');
  }

  // Parse the body document
  const { doc: body } = parseBsonDocument(bodySection.payload);

  // Extract database name
  const database = body['$db'] as string | undefined;
  if (!database) {
    throw new Error('$db field is required in OP_MSG command');
  }

  // Find the command name (first key that isn't a special field)
  let commandName: string | undefined;
  let collectionName: string | undefined;

  for (const key of Object.keys(body)) {
    if (key.startsWith('$')) continue; // Skip special fields like $db

    commandName = key;
    const value = body[key];

    // Check if this command takes a collection name
    if (COLLECTION_COMMANDS.has(key)) {
      if (typeof value === 'string') {
        collectionName = value;
        if (collectionName === '') {
          throw new Error('Collection name cannot be empty');
        }
      }
    }
    break;
  }

  if (!commandName) {
    throw new Error('Could not find command name in OP_MSG body');
  }

  // Build result
  const result: ExtractedCommand = {
    name: commandName,
    database,
    body,
  };

  if (collectionName !== undefined) {
    result.collection = collectionName;
  }

  // Handle getMore specially - collection comes from 'collection' field
  if (commandName === 'getMore') {
    result.cursorId = body.getMore as number;
    if (body.collection) {
      result.collection = body.collection as string;
    }
  }

  // Extract documents from type 1 sections
  const documentSequences = msg.sections.filter((s) => s.type === 1);
  if (documentSequences.length > 0) {
    result.documents = [];
    for (const section of documentSequences) {
      // Parse all documents in the sequence
      let pos = 0;
      while (pos < section.payload.length) {
        const { doc, bytesRead } = parseBsonDocument(section.payload, pos);
        result.documents.push(doc);
        pos += bytesRead;
      }
    }
  }

  // Extract options
  if (body.ordered !== undefined) {
    result.options = result.options || {};
    result.options.ordered = Boolean(body.ordered);
  }

  return result;
}

// ============================================================================
// Streaming Parser
// ============================================================================

/**
 * Streaming message parser for handling large messages incrementally.
 *
 * This parser maintains an internal buffer and allows feeding data in chunks.
 * It automatically handles message boundaries and can parse multiple messages
 * from a single stream.
 *
 * @example
 * ```typescript
 * const parser = new StreamingMessageParser();
 *
 * // Feed data as it arrives from the network
 * socket.on('data', (chunk) => {
 *   const result = parser.feed(chunk);
 *   if (result.message) {
 *     handleMessage(result.message);
 *   }
 * });
 * ```
 */
export class StreamingMessageParser {
  private buffer: Uint8Array;
  private bufferOffset: number = 0;
  private expectedLength: number | null = null;
  private state: StreamingParserState = 'awaiting_header';
  private lastError: Error | null = null;

  /**
   * Create a new streaming parser
   *
   * @param initialBufferSize - Initial buffer capacity (default 64KB)
   */
  constructor(initialBufferSize: number = 64 * 1024) {
    this.buffer = new Uint8Array(initialBufferSize);
  }

  /**
   * Get the current parser state
   */
  getState(): StreamingParserState {
    return this.state;
  }

  /**
   * Get the number of bytes currently buffered
   */
  getBufferedLength(): number {
    return this.bufferOffset;
  }

  /**
   * Get the expected message length (if known)
   */
  getExpectedLength(): number | null {
    return this.expectedLength;
  }

  /**
   * Reset the parser to initial state, clearing all buffered data
   */
  reset(): void {
    this.bufferOffset = 0;
    this.expectedLength = null;
    this.state = 'awaiting_header';
    this.lastError = null;
  }

  /**
   * Feed data to the parser and attempt to parse a complete message
   *
   * @param data - New data to add to the buffer
   * @returns Result indicating state and any parsed message
   */
  feed(data: Uint8Array): StreamingParserResult {
    // If we're in error state, don't accept more data
    if (this.state === 'error') {
      return {
        state: 'error',
        bytesConsumed: 0,
        error: this.lastError ?? new Error('Parser is in error state'),
      };
    }

    // Ensure buffer has enough capacity
    this.ensureCapacity(data.length);

    // Copy new data to buffer
    this.buffer.set(data, this.bufferOffset);
    this.bufferOffset += data.length;

    // Try to parse header if we don't know expected length
    if (this.state === 'awaiting_header' && this.bufferOffset >= HEADER_SIZE) {
      try {
        const view = new DataView(
          this.buffer.buffer,
          this.buffer.byteOffset,
          HEADER_SIZE
        );
        const messageLength = view.getInt32(0, true);

        // Validate message length
        if (messageLength < MIN_MESSAGE_SIZE) {
          throw new Error(
            `Invalid message length: ${messageLength} is less than minimum ${MIN_MESSAGE_SIZE}`
          );
        }

        if (messageLength > MAX_MESSAGE_SIZE) {
          throw new Error(
            `Message size exceeds maximum: ${messageLength} > ${MAX_MESSAGE_SIZE}`
          );
        }

        this.expectedLength = messageLength;
        this.state = 'awaiting_body';
      } catch (e) {
        this.state = 'error';
        this.lastError = e instanceof Error ? e : new Error(String(e));
        return {
          state: 'error',
          bytesConsumed: data.length,
          error: this.lastError,
        };
      }
    }

    // Check if we have a complete message
    if (
      this.state === 'awaiting_body' &&
      this.expectedLength !== null &&
      this.bufferOffset >= this.expectedLength
    ) {
      try {
        // Extract the message bytes
        const messageBytes = this.buffer.slice(0, this.expectedLength);

        // Parse the complete message
        const message = parseMessage(messageBytes);

        // Calculate bytes consumed
        const bytesConsumed = this.expectedLength;

        // Shift remaining data to beginning of buffer
        const remaining = this.bufferOffset - bytesConsumed;
        if (remaining > 0) {
          this.buffer.copyWithin(0, bytesConsumed, this.bufferOffset);
        }
        this.bufferOffset = remaining;

        // Reset state for next message
        this.expectedLength = null;
        this.state = 'awaiting_header';

        return {
          state: 'complete',
          message,
          bytesConsumed,
        };
      } catch (e) {
        this.state = 'error';
        this.lastError = e instanceof Error ? e : new Error(String(e));
        return {
          state: 'error',
          bytesConsumed: data.length,
          error: this.lastError,
        };
      }
    }

    // Still waiting for more data
    return {
      state: this.state,
      bytesConsumed: data.length,
    };
  }

  /**
   * Check if there might be more complete messages in the buffer
   *
   * @returns true if buffer contains enough data for at least a header
   */
  hasMoreMessages(): boolean {
    return this.bufferOffset >= HEADER_SIZE;
  }

  /**
   * Try to parse the next message from buffered data
   *
   * @returns Parsed message or null if incomplete
   */
  tryParseNext(): ParsedMessage | null {
    if (this.state === 'error') {
      return null;
    }

    // Feed empty data to trigger parsing attempt
    const result = this.feed(new Uint8Array(0));
    return result.message ?? null;
  }

  /**
   * Ensure the internal buffer has enough capacity for additional data
   */
  private ensureCapacity(additionalBytes: number): void {
    const required = this.bufferOffset + additionalBytes;
    if (required <= this.buffer.length) {
      return;
    }

    // Double the buffer size until it's large enough
    let newSize = this.buffer.length;
    while (newSize < required) {
      newSize *= 2;
    }

    // Cap at max message size + some overhead
    newSize = Math.min(newSize, MAX_MESSAGE_SIZE + 1024);

    const newBuffer = new Uint8Array(newSize);
    newBuffer.set(this.buffer.subarray(0, this.bufferOffset));
    this.buffer = newBuffer;
  }
}
