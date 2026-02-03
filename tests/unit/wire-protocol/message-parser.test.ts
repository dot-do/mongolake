/**
 * MongoDB Wire Protocol Message Parser Tests
 *
 * Tests for parsing MongoDB wire protocol messages, specifically:
 * - Message headers (16 bytes: length, requestId, responseTo, opCode)
 * - OP_MSG messages (opCode 2013) - the modern MongoDB message format
 * - Command document extraction
 * - Checksums and flags handling
 *
 * Wire Protocol Reference: https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/
 */

import { describe, it, expect } from 'vitest'

// Import the parser module
import {
  parseMessageHeader,
  parseOpMsg,
  parseMessage,
  extractCommand,
  StreamingMessageParser,
  MessageHeader,
  OpMsgMessage,
  OpMsgSection,
  OpCode,
  OpMsgFlags,
} from '../../../src/wire-protocol/message-parser'

/**
 * Helper to create a valid MongoDB wire protocol message header
 */
function createHeader(
  messageLength: number,
  requestId: number,
  responseTo: number,
  opCode: number
): Uint8Array {
  const buffer = new ArrayBuffer(16)
  const view = new DataView(buffer)
  // MongoDB uses little-endian byte order
  view.setInt32(0, messageLength, true)
  view.setInt32(4, requestId, true)
  view.setInt32(8, responseTo, true)
  view.setInt32(12, opCode, true)
  return new Uint8Array(buffer)
}

/**
 * Helper to create a BSON document for testing
 * This creates a minimal valid BSON document
 */
function createBsonDocument(doc: Record<string, unknown>): Uint8Array {
  // Simplified BSON encoder for testing
  // In reality, we'd use a proper BSON library
  const parts: Uint8Array[] = []

  for (const [key, value] of Object.entries(doc)) {
    if (typeof value === 'string') {
      // Type 0x02 = string
      const keyBytes = new TextEncoder().encode(key + '\0')
      const valueBytes = new TextEncoder().encode(value + '\0')
      const stringLen = valueBytes.length
      const element = new Uint8Array(1 + keyBytes.length + 4 + stringLen)
      element[0] = 0x02
      element.set(keyBytes, 1)
      const view = new DataView(element.buffer)
      view.setInt32(1 + keyBytes.length, stringLen, true)
      element.set(valueBytes, 1 + keyBytes.length + 4)
      parts.push(element)
    } else if (typeof value === 'number' && Number.isInteger(value)) {
      // Type 0x10 = int32
      const keyBytes = new TextEncoder().encode(key + '\0')
      const element = new Uint8Array(1 + keyBytes.length + 4)
      element[0] = 0x10
      element.set(keyBytes, 1)
      const view = new DataView(element.buffer)
      view.setInt32(1 + keyBytes.length, value, true)
      parts.push(element)
    }
  }

  // Calculate total document size: 4 (size) + elements + 1 (terminator)
  const elementsSize = parts.reduce((sum, p) => sum + p.length, 0)
  const docSize = 4 + elementsSize + 1

  const doc_bytes = new Uint8Array(docSize)
  const view = new DataView(doc_bytes.buffer)
  view.setInt32(0, docSize, true)

  let offset = 4
  for (const part of parts) {
    doc_bytes.set(part, offset)
    offset += part.length
  }
  doc_bytes[offset] = 0x00 // Document terminator

  return doc_bytes
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
  let sectionsSize = 0
  for (const section of sections) {
    if (section.type === 0) {
      sectionsSize += 1 + section.payload.length // type byte + document
    } else {
      // Type 1: Document sequence
      const idBytes = new TextEncoder().encode(section.identifier! + '\0')
      sectionsSize += 1 + 4 + idBytes.length + section.payload.length
    }
  }

  // Total message length: header (16) + flags (4) + sections
  const messageLength = 16 + 4 + sectionsSize

  const buffer = new Uint8Array(messageLength)
  const view = new DataView(buffer.buffer)

  // Header
  view.setInt32(0, messageLength, true)
  view.setInt32(4, requestId, true)
  view.setInt32(8, responseTo, true)
  view.setInt32(12, 2013, true) // OP_MSG opCode

  // Flags
  view.setUint32(16, flags, true)

  // Sections
  let offset = 20
  for (const section of sections) {
    buffer[offset] = section.type
    offset += 1

    if (section.type === 0) {
      buffer.set(section.payload, offset)
      offset += section.payload.length
    } else {
      const idBytes = new TextEncoder().encode(section.identifier! + '\0')
      const sectionSize = 4 + idBytes.length + section.payload.length
      view.setInt32(offset, sectionSize, true)
      offset += 4
      buffer.set(idBytes, offset)
      offset += idBytes.length
      buffer.set(section.payload, offset)
      offset += section.payload.length
    }
  }

  return buffer
}


describe('MongoDB Wire Protocol Message Parser', () => {

  describe('parseMessageHeader', () => {

    it('should parse a valid message header', () => {
      const header = createHeader(100, 1234, 0, 2013)

      const result = parseMessageHeader(header)

      expect(result).toEqual({
        messageLength: 100,
        requestId: 1234,
        responseTo: 0,
        opCode: 2013,
      })
    })

    it('should parse header with large request ID', () => {
      const header = createHeader(256, 0x7FFFFFFF, 5678, 2013)

      const result = parseMessageHeader(header)

      expect(result.requestId).toBe(0x7FFFFFFF)
    })

    it('should parse header with responseTo set', () => {
      const header = createHeader(200, 100, 99, 2013)

      const result = parseMessageHeader(header)

      expect(result.responseTo).toBe(99)
    })

    it('should correctly identify OP_MSG opCode (2013)', () => {
      const header = createHeader(50, 1, 0, OpCode.OP_MSG)

      const result = parseMessageHeader(header)

      expect(result.opCode).toBe(OpCode.OP_MSG)
      expect(result.opCode).toBe(2013)
    })

    it('should correctly identify legacy opCodes', () => {
      // OP_QUERY (deprecated but may still be encountered)
      const queryHeader = createHeader(50, 1, 0, 2004)
      expect(parseMessageHeader(queryHeader).opCode).toBe(2004)

      // OP_REPLY (deprecated)
      const replyHeader = createHeader(50, 1, 0, 1)
      expect(parseMessageHeader(replyHeader).opCode).toBe(1)
    })

    it('should throw error for buffer smaller than 16 bytes', () => {
      const smallBuffer = new Uint8Array(10)

      expect(() => parseMessageHeader(smallBuffer)).toThrow()
    })

    it('should accept buffer with offset parameter', () => {
      const buffer = new Uint8Array(32)
      // Put header at offset 16
      const header = createHeader(100, 42, 0, 2013)
      buffer.set(header, 16)

      const result = parseMessageHeader(buffer, 16)

      expect(result.requestId).toBe(42)
    })

    it('should handle maximum message length (48MB)', () => {
      const maxLength = 48 * 1024 * 1024 // 48MB
      const header = createHeader(maxLength, 1, 0, 2013)

      const result = parseMessageHeader(header)

      expect(result.messageLength).toBe(maxLength)
    })

  })


  describe('parseOpMsg', () => {

    it('should parse OP_MSG with single body section (type 0)', () => {
      const doc = createBsonDocument({ find: 'users', filter: '{}' })
      const message = createOpMsg(1, 0, 0, [
        { type: 0, payload: doc }
      ])

      const result = parseOpMsg(message)

      expect(result.flags).toBe(0)
      expect(result.sections).toHaveLength(1)
      expect(result.sections[0].type).toBe(0)
    })

    it('should parse OP_MSG with checksumPresent flag', () => {
      const doc = createBsonDocument({ ping: 1 })
      const message = createOpMsg(1, 0, OpMsgFlags.checksumPresent, [
        { type: 0, payload: doc }
      ])
      // Append 4 bytes for CRC32 checksum
      const withChecksum = new Uint8Array(message.length + 4)
      withChecksum.set(message)
      // Update message length
      const view = new DataView(withChecksum.buffer)
      view.setInt32(0, withChecksum.length, true)
      // Set a dummy checksum
      view.setUint32(withChecksum.length - 4, 0x12345678, true)

      const result = parseOpMsg(withChecksum)

      expect(result.flags & OpMsgFlags.checksumPresent).toBeTruthy()
      expect(result.checksum).toBe(0x12345678)
    })

    it('should parse OP_MSG with moreToCome flag', () => {
      const doc = createBsonDocument({ getMore: 1 })
      const message = createOpMsg(1, 0, OpMsgFlags.moreToCome, [
        { type: 0, payload: doc }
      ])

      const result = parseOpMsg(message)

      expect(result.flags & OpMsgFlags.moreToCome).toBeTruthy()
    })

    it('should parse OP_MSG with exhaustAllowed flag', () => {
      const doc = createBsonDocument({ find: 'test' })
      const message = createOpMsg(1, 0, OpMsgFlags.exhaustAllowed, [
        { type: 0, payload: doc }
      ])

      const result = parseOpMsg(message)

      expect(result.flags & OpMsgFlags.exhaustAllowed).toBeTruthy()
    })

    it('should parse OP_MSG with document sequence section (type 1)', () => {
      const doc1 = createBsonDocument({ x: 1 })
      const doc2 = createBsonDocument({ x: 2 })
      const docs = new Uint8Array(doc1.length + doc2.length)
      docs.set(doc1, 0)
      docs.set(doc2, doc1.length)

      const bodyDoc = createBsonDocument({ insert: 'test', '$db': 'testdb' })
      const message = createOpMsg(1, 0, 0, [
        { type: 0, payload: bodyDoc },
        { type: 1, payload: docs, identifier: 'documents' }
      ])

      const result = parseOpMsg(message)

      expect(result.sections).toHaveLength(2)
      expect(result.sections[1].type).toBe(1)
      expect((result.sections[1] as OpMsgSection).identifier).toBe('documents')
    })

    it('should parse OP_MSG with multiple document sequences', () => {
      const bodyDoc = createBsonDocument({ update: 'test', '$db': 'testdb' })
      const updateDoc = createBsonDocument({ q: '{}', u: '{}' })
      const arrayFilter = createBsonDocument({ elem: 1 })

      const message = createOpMsg(1, 0, 0, [
        { type: 0, payload: bodyDoc },
        { type: 1, payload: updateDoc, identifier: 'updates' },
        { type: 1, payload: arrayFilter, identifier: 'arrayFilters' }
      ])

      const result = parseOpMsg(message)

      expect(result.sections).toHaveLength(3)
      expect(result.sections.filter(s => s.type === 1)).toHaveLength(2)
    })

    it('should throw error for invalid opCode', () => {
      const header = createHeader(50, 1, 0, 2004) // OP_QUERY, not OP_MSG

      expect(() => parseOpMsg(header)).toThrow(/opCode/)
    })

    it('should throw error for unknown section type', () => {
      const doc = createBsonDocument({ ping: 1 })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])
      // Corrupt section type to invalid value
      message[20] = 5 // Invalid section type

      expect(() => parseOpMsg(message)).toThrow(/section type/)
    })

  })


  describe('parseMessage', () => {

    it('should auto-detect and parse OP_MSG', () => {
      const doc = createBsonDocument({ ping: 1 })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      const result = parseMessage(message)

      expect(result.header.opCode).toBe(OpCode.OP_MSG)
      expect(result.type).toBe('OP_MSG')
    })

    it('should return header and raw body for unsupported opCodes', () => {
      const header = createHeader(20, 1, 0, 2001) // OP_UPDATE (deprecated, unsupported)
      const message = new Uint8Array(20)
      message.set(header)

      const result = parseMessage(message)

      expect(result.header.opCode).toBe(2001)
      expect(result.type).toBe('UNKNOWN')
    })

    it('should handle message with exact length', () => {
      const doc = createBsonDocument({ hello: 1 })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      // Verify length matches
      const view = new DataView(message.buffer)
      expect(view.getInt32(0, true)).toBe(message.length)

      const result = parseMessage(message)
      expect(result).toBeDefined()
    })

  })


  describe('Handle incomplete messages', () => {

    it('should throw error when message is shorter than header', () => {
      const incompleteHeader = new Uint8Array(10)

      expect(() => parseMessage(incompleteHeader)).toThrow(/incomplete/)
    })

    it('should throw error when message length field exceeds actual data', () => {
      // Header claims 1000 bytes, but we only have 20
      const header = createHeader(1000, 1, 0, 2013)
      const message = new Uint8Array(20)
      message.set(header)

      expect(() => parseMessage(message)).toThrow(/incomplete|truncated/)
    })

    it('should throw error when OP_MSG sections are truncated', () => {
      const doc = createBsonDocument({ ping: 1 })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])
      // Truncate the message
      const truncated = message.slice(0, message.length - 5)
      // Fix length header to match truncated size
      const view = new DataView(truncated.buffer)
      view.setInt32(0, truncated.length, true)

      expect(() => parseOpMsg(truncated)).toThrow()
    })

    it('should detect incomplete BSON document in section', () => {
      // Create a message where the BSON document size claims more bytes than available
      const message = new Uint8Array(30)
      const view = new DataView(message.buffer)

      // Header
      view.setInt32(0, 30, true)
      view.setInt32(4, 1, true)
      view.setInt32(8, 0, true)
      view.setInt32(12, 2013, true)

      // Flags
      view.setUint32(16, 0, true)

      // Section type 0
      message[20] = 0

      // BSON document claiming to be 100 bytes (but only ~10 available)
      view.setInt32(21, 100, true)

      expect(() => parseOpMsg(message)).toThrow()
    })

  })


  describe('Handle invalid messages', () => {

    it('should throw error for negative message length', () => {
      const header = createHeader(-1, 1, 0, 2013)

      expect(() => parseMessageHeader(header)).toThrow(/invalid.*length/i)
    })

    it('should throw error for message length less than header size', () => {
      const header = createHeader(10, 1, 0, 2013) // Less than 16 bytes

      expect(() => parseMessage(header)).toThrow(/invalid.*length/i)
    })

    it('should throw error for message length exceeding maximum (48MB)', () => {
      const tooLarge = 50 * 1024 * 1024 // 50MB
      const header = createHeader(tooLarge, 1, 0, 2013)

      expect(() => parseMessage(header)).toThrow(/exceeds maximum/i)
    })

    it('should throw error for zero-length message', () => {
      const header = createHeader(0, 1, 0, 2013)

      expect(() => parseMessage(header)).toThrow(/invalid.*length/i)
    })

    it('should throw error for corrupted BSON in body section', () => {
      const message = new Uint8Array(30)
      const view = new DataView(message.buffer)

      // Valid header
      view.setInt32(0, 30, true)
      view.setInt32(4, 1, true)
      view.setInt32(8, 0, true)
      view.setInt32(12, 2013, true)
      view.setUint32(16, 0, true)

      // Section type 0
      message[20] = 0

      // Invalid BSON: document size matches but no terminator
      view.setInt32(21, 9, true) // Claims 9 bytes
      // Fill with garbage, missing 0x00 terminator
      message[25] = 0xFF
      message[26] = 0xFF
      message[27] = 0xFF
      message[28] = 0xFF
      message[29] = 0xFF // Should be 0x00

      expect(() => parseOpMsg(message)).toThrow(/BSON|invalid/i)
    })

    it('should reject reserved flag bits being set', () => {
      const doc = createBsonDocument({ ping: 1 })
      // Set reserved bits (bits 3-15 must be 0)
      const message = createOpMsg(1, 0, 0b1111111100000000, [
        { type: 0, payload: doc }
      ])

      expect(() => parseOpMsg(message)).toThrow(/reserved.*flag/i)
    })

  })


  describe('extractCommand', () => {

    it('should extract find command from OP_MSG', () => {
      const doc = createBsonDocument({
        find: 'users',
        '$db': 'testdb'
      })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])
      const parsed = parseOpMsg(message)

      const command = extractCommand(parsed)

      expect(command.name).toBe('find')
      expect(command.collection).toBe('users')
      expect(command.database).toBe('testdb')
    })

    it('should extract insert command with documents', () => {
      const bodyDoc = createBsonDocument({
        insert: 'products',
        '$db': 'shop'
      })
      const doc1 = createBsonDocument({ name: 'widget', price: 100 })

      const message = createOpMsg(1, 0, 0, [
        { type: 0, payload: bodyDoc },
        { type: 1, payload: doc1, identifier: 'documents' }
      ])
      const parsed = parseOpMsg(message)

      const command = extractCommand(parsed)

      expect(command.name).toBe('insert')
      expect(command.collection).toBe('products')
      expect(command.documents).toBeDefined()
    })

    it('should extract update command', () => {
      const bodyDoc = createBsonDocument({
        update: 'users',
        '$db': 'testdb'
      })
      const updateDoc = createBsonDocument({ q: '{}', u: '{}' })

      const message = createOpMsg(1, 0, 0, [
        { type: 0, payload: bodyDoc },
        { type: 1, payload: updateDoc, identifier: 'updates' }
      ])
      const parsed = parseOpMsg(message)

      const command = extractCommand(parsed)

      expect(command.name).toBe('update')
      expect(command.collection).toBe('users')
    })

    it('should extract delete command', () => {
      const bodyDoc = createBsonDocument({
        delete: 'sessions',
        '$db': 'testdb'
      })

      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: bodyDoc }])
      const parsed = parseOpMsg(message)

      const command = extractCommand(parsed)

      expect(command.name).toBe('delete')
      expect(command.collection).toBe('sessions')
    })

    it('should extract aggregate command', () => {
      const bodyDoc = createBsonDocument({
        aggregate: 'orders',
        '$db': 'analytics'
      })

      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: bodyDoc }])
      const parsed = parseOpMsg(message)

      const command = extractCommand(parsed)

      expect(command.name).toBe('aggregate')
      expect(command.collection).toBe('orders')
      expect(command.database).toBe('analytics')
    })

    it('should extract getMore command with cursorId', () => {
      const bodyDoc = createBsonDocument({
        getMore: 12345, // cursor ID
        collection: 'users',
        '$db': 'testdb'
      })

      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: bodyDoc }])
      const parsed = parseOpMsg(message)

      const command = extractCommand(parsed)

      expect(command.name).toBe('getMore')
      expect(command.cursorId).toBe(12345)
    })

    it('should extract admin commands (ping, hello, etc.)', () => {
      const pingDoc = createBsonDocument({ ping: 1, '$db': 'admin' })
      const pingMessage = createOpMsg(1, 0, 0, [{ type: 0, payload: pingDoc }])

      const helloDoc = createBsonDocument({ hello: 1, '$db': 'admin' })
      const helloMessage = createOpMsg(2, 0, 0, [{ type: 0, payload: helloDoc }])

      expect(extractCommand(parseOpMsg(pingMessage)).name).toBe('ping')
      expect(extractCommand(parseOpMsg(helloMessage)).name).toBe('hello')
    })

    it('should throw error for OP_MSG without body section', () => {
      // Create an OP_MSG with only document sequence sections
      const doc = createBsonDocument({ x: 1 })
      const message = createOpMsg(1, 0, 0, [
        { type: 1, payload: doc, identifier: 'documents' }
      ])
      const parsed = parseOpMsg(message)

      expect(() => extractCommand(parsed)).toThrow(/body section/)
    })

  })


  describe('Parse real MongoDB commands', () => {

    it('should parse find command with filter and projection', () => {
      const doc = createBsonDocument({
        find: 'users',
        '$db': 'myapp'
      })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      const parsed = parseOpMsg(message)
      const command = extractCommand(parsed)

      expect(command.name).toBe('find')
      expect(command.collection).toBe('users')
    })

    it('should parse insert command with ordered flag', () => {
      const doc = createBsonDocument({
        insert: 'events',
        ordered: 1,
        '$db': 'logs'
      })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      const command = extractCommand(parseOpMsg(message))

      expect(command.name).toBe('insert')
      expect(command.options?.ordered).toBe(true)
    })

    it('should parse createIndexes command', () => {
      const doc = createBsonDocument({
        createIndexes: 'users',
        '$db': 'myapp'
      })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      const command = extractCommand(parseOpMsg(message))

      expect(command.name).toBe('createIndexes')
      expect(command.collection).toBe('users')
    })

    it('should parse drop command', () => {
      const doc = createBsonDocument({
        drop: 'temp_data',
        '$db': 'testdb'
      })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      const command = extractCommand(parseOpMsg(message))

      expect(command.name).toBe('drop')
      expect(command.collection).toBe('temp_data')
    })

    it('should parse listCollections command', () => {
      const doc = createBsonDocument({
        listCollections: 1,
        '$db': 'myapp'
      })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      const command = extractCommand(parseOpMsg(message))

      expect(command.name).toBe('listCollections')
      expect(command.database).toBe('myapp')
    })

    it('should parse count command', () => {
      const doc = createBsonDocument({
        count: 'orders',
        '$db': 'shop'
      })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      const command = extractCommand(parseOpMsg(message))

      expect(command.name).toBe('count')
      expect(command.collection).toBe('orders')
    })

    it('should parse distinct command', () => {
      const doc = createBsonDocument({
        distinct: 'users',
        key: 'status',
        '$db': 'myapp'
      })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      const command = extractCommand(parseOpMsg(message))

      expect(command.name).toBe('distinct')
      expect(command.collection).toBe('users')
    })

    it('should handle isMaster/ismaster legacy command', () => {
      const doc = createBsonDocument({
        isMaster: 1,
        '$db': 'admin'
      })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      const command = extractCommand(parseOpMsg(message))

      expect(command.name).toBe('isMaster')
    })

    it('should handle buildInfo command', () => {
      const doc = createBsonDocument({
        buildInfo: 1,
        '$db': 'admin'
      })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      const command = extractCommand(parseOpMsg(message))

      expect(command.name).toBe('buildInfo')
    })

  })


  describe('Edge cases and boundary conditions', () => {

    it('should handle empty collection name', () => {
      const doc = createBsonDocument({
        find: '',
        '$db': 'test'
      })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      expect(() => extractCommand(parseOpMsg(message))).toThrow(/collection.*empty/i)
    })

    it('should handle missing $db field', () => {
      const doc = createBsonDocument({
        find: 'users'
        // Missing $db
      })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      expect(() => extractCommand(parseOpMsg(message))).toThrow(/\$db.*required/i)
    })

    it('should handle very long collection names', () => {
      const longName = 'a'.repeat(200)
      const doc = createBsonDocument({
        find: longName,
        '$db': 'test'
      })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      const command = extractCommand(parseOpMsg(message))

      expect(command.collection).toBe(longName)
    })

    it('should handle maximum request ID', () => {
      const doc = createBsonDocument({ ping: 1, '$db': 'admin' })
      const message = createOpMsg(0xFFFFFFFF, 0, 0, [{ type: 0, payload: doc }])

      const parsed = parseOpMsg(message)

      expect(parsed.header.requestId).toBe(0xFFFFFFFF)
    })

    it('should handle concurrent flag combinations', () => {
      const doc = createBsonDocument({ ping: 1, '$db': 'admin' })
      const flags = OpMsgFlags.checksumPresent | OpMsgFlags.moreToCome
      const message = createOpMsg(1, 0, flags, [{ type: 0, payload: doc }])
      // Add checksum
      const withChecksum = new Uint8Array(message.length + 4)
      withChecksum.set(message)
      const view = new DataView(withChecksum.buffer)
      view.setInt32(0, withChecksum.length, true)
      view.setUint32(withChecksum.length - 4, 0xDEADBEEF, true)

      const parsed = parseOpMsg(withChecksum)

      expect(parsed.flags & OpMsgFlags.checksumPresent).toBeTruthy()
      expect(parsed.flags & OpMsgFlags.moreToCome).toBeTruthy()
    })

    it('should reject exhaustAllowed with moreToCome', () => {
      // These flags cannot be used together
      const doc = createBsonDocument({ ping: 1, '$db': 'admin' })
      const flags = OpMsgFlags.exhaustAllowed | OpMsgFlags.moreToCome
      const message = createOpMsg(1, 0, flags, [{ type: 0, payload: doc }])

      expect(() => parseOpMsg(message)).toThrow(/exhaustAllowed.*moreToCome/i)
    })

  })


  // ==========================================================================
  // Additional Error Scenarios
  // ==========================================================================

  describe('BSON parsing errors', () => {

    it('should throw for BSON document with size smaller than minimum (5 bytes)', () => {
      const message = new Uint8Array(30)
      const view = new DataView(message.buffer)

      // Valid header
      view.setInt32(0, 30, true)
      view.setInt32(4, 1, true)
      view.setInt32(8, 0, true)
      view.setInt32(12, 2013, true)
      view.setUint32(16, 0, true)

      // Section type 0
      message[20] = 0

      // BSON document with size < 5 (invalid)
      view.setInt32(21, 3, true)

      expect(() => parseOpMsg(message)).toThrow(/invalid.*BSON/i)
    })

    it('should throw for BSON document with size exceeding buffer', () => {
      const message = new Uint8Array(30)
      const view = new DataView(message.buffer)

      // Valid header
      view.setInt32(0, 30, true)
      view.setInt32(4, 1, true)
      view.setInt32(8, 0, true)
      view.setInt32(12, 2013, true)
      view.setUint32(16, 0, true)

      // Section type 0
      message[20] = 0

      // BSON document claiming 1000 bytes (exceeds buffer)
      view.setInt32(21, 1000, true)

      expect(() => parseOpMsg(message)).toThrow()
    })

    it('should throw for BSON document missing terminator', () => {
      const message = new Uint8Array(30)
      const view = new DataView(message.buffer)

      // Valid header
      view.setInt32(0, 30, true)
      view.setInt32(4, 1, true)
      view.setInt32(8, 0, true)
      view.setInt32(12, 2013, true)
      view.setUint32(16, 0, true)

      // Section type 0
      message[20] = 0

      // BSON document with size 8, but missing terminator
      view.setInt32(21, 8, true)
      message[28] = 0xFF // Should be 0x00

      expect(() => parseOpMsg(message)).toThrow(/BSON/i)
    })

    it('should throw for unsupported BSON element type', () => {
      const message = new Uint8Array(40)
      const view = new DataView(message.buffer)

      // Valid header
      view.setInt32(0, 40, true)
      view.setInt32(4, 1, true)
      view.setInt32(8, 0, true)
      view.setInt32(12, 2013, true)
      view.setUint32(16, 0, true)

      // Section type 0
      message[20] = 0

      // BSON document with unsupported type (0xFF = MinKey requires special handling)
      view.setInt32(21, 15, true) // doc size
      message[25] = 0x80 // Invalid BSON type
      message[26] = 0x78 // 'x'
      message[27] = 0x00 // null terminator for field name
      // Some data bytes
      message[35] = 0x00 // doc terminator

      expect(() => parseOpMsg(message)).toThrow()
    })

  })


  describe('Protocol boundary errors', () => {

    it('should throw for message length exactly 16 (header only, no body)', () => {
      const header = createHeader(16, 1, 0, 2013)

      // Should throw since header-only message has no body data
      expect(() => parseMessage(header)).toThrow()
    })

    it('should handle message at exactly minimum valid size', () => {
      // Minimum OP_MSG: header(16) + flags(4) + section_type(1) + bson_doc(5) = 26
      const minDoc = new Uint8Array(5)
      new DataView(minDoc.buffer).setInt32(0, 5, true) // doc size
      minDoc[4] = 0x00 // terminator

      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: minDoc }])

      const result = parseMessage(message)
      expect(result.type).toBe('OP_MSG')
    })

    it('should throw for truncated flags field', () => {
      const header = createHeader(18, 1, 0, 2013) // Only 2 bytes for flags
      const message = new Uint8Array(18)
      message.set(header)

      expect(() => parseOpMsg(message)).toThrow()
    })

    it('should throw for section type byte with no following data', () => {
      const message = new Uint8Array(21)
      const view = new DataView(message.buffer)

      view.setInt32(0, 21, true)
      view.setInt32(4, 1, true)
      view.setInt32(8, 0, true)
      view.setInt32(12, 2013, true)
      view.setUint32(16, 0, true)
      message[20] = 0 // section type but no BSON document

      expect(() => parseOpMsg(message)).toThrow()
    })

  })


  describe('Malformed message structures', () => {

    it('should handle message with multiple body sections (type 0)', () => {
      // Per spec, only one type-0 section is allowed
      const doc1 = createBsonDocument({ ping: 1, '$db': 'admin' })
      const doc2 = createBsonDocument({ hello: 1, '$db': 'admin' })

      // Manually construct message with two type-0 sections
      const messageLength = 16 + 4 + 1 + doc1.length + 1 + doc2.length
      const message = new Uint8Array(messageLength)
      const view = new DataView(message.buffer)

      view.setInt32(0, messageLength, true)
      view.setInt32(4, 1, true)
      view.setInt32(8, 0, true)
      view.setInt32(12, 2013, true)
      view.setUint32(16, 0, true)

      let offset = 20
      message[offset++] = 0 // section type 0
      message.set(doc1, offset)
      offset += doc1.length
      message[offset++] = 0 // another section type 0
      message.set(doc2, offset)

      // This should still parse (extractCommand takes first body section)
      const parsed = parseOpMsg(message)
      expect(parsed.sections.filter(s => s.type === 0)).toHaveLength(2)
    })

    it('should handle type-1 section with empty identifier', () => {
      const bodyDoc = createBsonDocument({ insert: 'test', '$db': 'testdb' })
      const docPayload = createBsonDocument({ x: 1 })

      // Manually create message with empty identifier in type-1 section
      const sectionSize = 4 + 1 + docPayload.length // size + null terminator + payload
      const messageLength = 16 + 4 + 1 + bodyDoc.length + 1 + sectionSize
      const message = new Uint8Array(messageLength)
      const view = new DataView(message.buffer)

      view.setInt32(0, messageLength, true)
      view.setInt32(4, 1, true)
      view.setInt32(8, 0, true)
      view.setInt32(12, 2013, true)
      view.setUint32(16, 0, true)

      let offset = 20
      message[offset++] = 0
      message.set(bodyDoc, offset)
      offset += bodyDoc.length
      message[offset++] = 1 // section type 1
      view.setInt32(offset, sectionSize, true)
      offset += 4
      message[offset++] = 0x00 // empty identifier (just null terminator)
      message.set(docPayload, offset)

      const parsed = parseOpMsg(message)
      const seq = parsed.sections.find(s => s.type === 1)
      expect(seq?.identifier).toBe('')
    })

    it('should handle type-1 section with very long identifier', () => {
      const bodyDoc = createBsonDocument({ insert: 'test', '$db': 'testdb' })
      const docPayload = createBsonDocument({ x: 1 })
      const longIdentifier = 'a'.repeat(1000)
      const idBytes = new TextEncoder().encode(longIdentifier + '\0')

      const sectionSize = 4 + idBytes.length + docPayload.length
      const messageLength = 16 + 4 + 1 + bodyDoc.length + 1 + sectionSize
      const message = new Uint8Array(messageLength)
      const view = new DataView(message.buffer)

      view.setInt32(0, messageLength, true)
      view.setInt32(4, 1, true)
      view.setInt32(8, 0, true)
      view.setInt32(12, 2013, true)
      view.setUint32(16, 0, true)

      let offset = 20
      message[offset++] = 0
      message.set(bodyDoc, offset)
      offset += bodyDoc.length
      message[offset++] = 1
      view.setInt32(offset, sectionSize, true)
      offset += 4
      message.set(idBytes, offset)
      offset += idBytes.length
      message.set(docPayload, offset)

      const parsed = parseOpMsg(message)
      const seq = parsed.sections.find(s => s.type === 1)
      expect(seq?.identifier).toBe(longIdentifier)
    })

  })


  describe('Checksum validation', () => {

    it('should extract checksum when checksumPresent flag is set', () => {
      const doc = createBsonDocument({ ping: 1, '$db': 'admin' })
      const baseMessage = createOpMsg(1, 0, OpMsgFlags.checksumPresent, [
        { type: 0, payload: doc }
      ])

      // Append checksum
      const withChecksum = new Uint8Array(baseMessage.length + 4)
      withChecksum.set(baseMessage)
      const view = new DataView(withChecksum.buffer)
      view.setInt32(0, withChecksum.length, true)
      view.setUint32(withChecksum.length - 4, 0xCAFEBABE, true)

      const parsed = parseOpMsg(withChecksum)

      expect(parsed.checksum).toBe(0xCAFEBABE)
    })

    it('should not have checksum when flag is not set', () => {
      const doc = createBsonDocument({ ping: 1, '$db': 'admin' })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      const parsed = parseOpMsg(message)

      expect(parsed.checksum).toBeUndefined()
    })

  })


  describe('OP_QUERY parsing errors', () => {

    it('should throw for malformed OP_QUERY message', () => {
      // parseMessage parses OP_QUERY and throws for invalid/truncated data
      const header = createHeader(50, 1, 0, 2004) // OP_QUERY opCode
      const message = new Uint8Array(50)
      message.set(header)
      // Body is all zeros which is not valid

      expect(() => parseMessage(message)).toThrow()
    })

    it('should throw for truncated OP_QUERY message', () => {
      // OP_QUERY needs at least: header(16) + flags(4) + collection + skip(4) + return(4) + doc
      const header = createHeader(20, 1, 0, 2004) // OP_QUERY but only 20 bytes
      const message = new Uint8Array(20)
      message.set(header)

      // Should throw due to insufficient data
      expect(() => parseMessage(message)).toThrow()
    })

  })


  describe('Command extraction edge cases', () => {

    it('should throw when command body has only special fields', () => {
      const doc = createBsonDocument({
        '$db': 'admin',
        '$readPreference': 'primary'
      })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      expect(() => extractCommand(parseOpMsg(message))).toThrow(/command name/i)
    })

    it('should extract command from first non-special field', () => {
      const doc = createBsonDocument({
        '$db': 'admin',
        ping: 1,
        '$readPreference': 'primary'
      })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      const command = extractCommand(parseOpMsg(message))
      expect(command.name).toBe('ping')
    })

    it('should handle getMore with missing collection field', () => {
      const doc = createBsonDocument({
        getMore: 12345,
        '$db': 'testdb'
        // Missing 'collection' field
      })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      const command = extractCommand(parseOpMsg(message))
      expect(command.name).toBe('getMore')
      expect(command.cursorId).toBe(12345)
      expect(command.collection).toBeUndefined()
    })

  })


  describe('Memory and resource constraints', () => {

    it('should handle message at maximum size boundary', () => {
      // Create a message close to the 48MB limit
      // We won't actually allocate 48MB, just test the validation
      const header = createHeader(48 * 1024 * 1024, 1, 0, 2013)

      // parseMessage will throw because we only have a 16-byte header
      // but claim 48MB length - it detects incomplete/truncated message
      expect(() => parseMessage(header)).toThrow()
    })

    it('should handle deeply nested BSON documents', () => {
      // Create a document with nested objects
      // Note: This tests the parser's ability to handle recursion
      let nested: Record<string, unknown> = { value: 1 }
      for (let i = 0; i < 10; i++) {
        nested = { nested }
      }
      nested['$db'] = 'test'
      nested['find'] = 'collection'

      const doc = createBsonDocument(nested as Record<string, unknown>)
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      // Should parse without stack overflow
      const parsed = parseOpMsg(message)
      expect(parsed.sections.length).toBe(1)
    })

  })


  // ==========================================================================
  // Streaming Parser Tests
  // ==========================================================================

  describe('StreamingMessageParser', () => {

    it('should parse a complete message fed all at once', () => {
      const parser = new StreamingMessageParser()
      const doc = createBsonDocument({ ping: 1, '$db': 'admin' })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      const result = parser.feed(message)

      expect(result.state).toBe('complete')
      expect(result.message).toBeDefined()
      expect(result.message?.type).toBe('OP_MSG')
      expect(result.bytesConsumed).toBe(message.length)
    })

    it('should handle message split across multiple chunks', () => {
      const parser = new StreamingMessageParser()
      const doc = createBsonDocument({ ping: 1, '$db': 'admin' })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      // Split the message after the header (16 bytes)
      const chunk1 = message.slice(0, 20) // header + part of flags
      const chunk2 = message.slice(20)

      // First chunk - header parsed, awaiting body
      const result1 = parser.feed(chunk1)
      expect(result1.state).toBe('awaiting_body')
      expect(result1.message).toBeUndefined()

      // Second chunk should complete the message
      const result2 = parser.feed(chunk2)
      expect(result2.state).toBe('complete')
      expect(result2.message).toBeDefined()
      expect(result2.message?.type).toBe('OP_MSG')
    })

    it('should handle header split across chunks', () => {
      const parser = new StreamingMessageParser()
      const doc = createBsonDocument({ ping: 1, '$db': 'admin' })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      // Split in the middle of the header
      const chunk1 = message.slice(0, 8)
      const chunk2 = message.slice(8)

      // First chunk - still awaiting header
      const result1 = parser.feed(chunk1)
      expect(result1.state).toBe('awaiting_header')

      // Second chunk should complete
      const result2 = parser.feed(chunk2)
      expect(result2.state).toBe('complete')
      expect(result2.message).toBeDefined()
    })

    it('should parse multiple messages in sequence', () => {
      const parser = new StreamingMessageParser()
      const doc = createBsonDocument({ ping: 1, '$db': 'admin' })
      const message1 = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])
      const message2 = createOpMsg(2, 0, 0, [{ type: 0, payload: doc }])

      // Parse first message
      const result1 = parser.feed(message1)
      expect(result1.state).toBe('complete')
      expect(result1.message?.header.requestId).toBe(1)

      // Parse second message
      const result2 = parser.feed(message2)
      expect(result2.state).toBe('complete')
      expect(result2.message?.header.requestId).toBe(2)
    })

    it('should handle multiple messages fed in a single chunk', () => {
      const parser = new StreamingMessageParser()
      const doc = createBsonDocument({ ping: 1, '$db': 'admin' })
      const message1 = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])
      const message2 = createOpMsg(2, 0, 0, [{ type: 0, payload: doc }])

      // Concatenate both messages
      const combined = new Uint8Array(message1.length + message2.length)
      combined.set(message1, 0)
      combined.set(message2, message1.length)

      // Feed everything at once
      const result1 = parser.feed(combined)
      expect(result1.state).toBe('complete')
      expect(result1.message?.header.requestId).toBe(1)
      expect(parser.hasMoreMessages()).toBe(true)

      // Parse the second message from buffer
      const result2 = parser.tryParseNext()
      expect(result2).toBeDefined()
      expect(result2?.header.requestId).toBe(2)
    })

    it('should report error for invalid message length', () => {
      const parser = new StreamingMessageParser()
      const badHeader = createHeader(-1, 1, 0, 2013)

      const result = parser.feed(badHeader)

      expect(result.state).toBe('error')
      expect(result.error).toBeDefined()
      expect(parser.getState()).toBe('error')
    })

    it('should report error for oversized message', () => {
      const parser = new StreamingMessageParser()
      const oversizedHeader = createHeader(50 * 1024 * 1024, 1, 0, 2013) // 50MB

      const result = parser.feed(oversizedHeader)

      expect(result.state).toBe('error')
      expect(result.error?.message).toMatch(/exceeds maximum/i)
    })

    it('should reset state properly', () => {
      const parser = new StreamingMessageParser()

      // Feed partial data
      parser.feed(new Uint8Array(10))
      expect(parser.getBufferedLength()).toBe(10)

      // Reset
      parser.reset()
      expect(parser.getBufferedLength()).toBe(0)
      expect(parser.getState()).toBe('awaiting_header')
      expect(parser.getExpectedLength()).toBeNull()
    })

    it('should track expected length after parsing header', () => {
      const parser = new StreamingMessageParser()
      const doc = createBsonDocument({ ping: 1, '$db': 'admin' })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      // Feed just the header
      parser.feed(message.slice(0, 16))

      expect(parser.getState()).toBe('awaiting_body')
      expect(parser.getExpectedLength()).toBe(message.length)
    })

    it('should not accept more data after error', () => {
      const parser = new StreamingMessageParser()

      // Create an error condition
      const badHeader = createHeader(-1, 1, 0, 2013)
      parser.feed(badHeader)

      expect(parser.getState()).toBe('error')

      // Try to feed more data
      const result = parser.feed(new Uint8Array(100))
      expect(result.state).toBe('error')
      expect(result.bytesConsumed).toBe(0)
    })

    it('should recover after reset from error state', () => {
      const parser = new StreamingMessageParser()

      // Create an error condition
      const badHeader = createHeader(-1, 1, 0, 2013)
      parser.feed(badHeader)
      expect(parser.getState()).toBe('error')

      // Reset and feed valid data
      parser.reset()
      const doc = createBsonDocument({ ping: 1, '$db': 'admin' })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      const result = parser.feed(message)
      expect(result.state).toBe('complete')
      expect(result.message).toBeDefined()
    })

    it('should handle byte-by-byte feeding', () => {
      const parser = new StreamingMessageParser()
      const doc = createBsonDocument({ ping: 1, '$db': 'admin' })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }])

      // Feed one byte at a time
      let lastResult
      for (let i = 0; i < message.length; i++) {
        lastResult = parser.feed(message.slice(i, i + 1))
        if (lastResult.state === 'complete') {
          break
        }
      }

      expect(lastResult?.state).toBe('complete')
      expect(lastResult?.message).toBeDefined()
    })

    it('should grow buffer automatically for large messages', () => {
      // Use small initial buffer
      const parser = new StreamingMessageParser(64)

      // Create a message larger than initial buffer
      const largeDoc = createBsonDocument({
        data: 'x'.repeat(1000),
        '$db': 'test'
      })
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: largeDoc }])

      const result = parser.feed(message)
      expect(result.state).toBe('complete')
      expect(result.message).toBeDefined()
    })

    it('should return null from tryParseNext when no complete message', () => {
      const parser = new StreamingMessageParser()

      // Feed partial data
      parser.feed(new Uint8Array(10))

      const result = parser.tryParseNext()
      expect(result).toBeNull()
    })

    it('should handle empty feed', () => {
      const parser = new StreamingMessageParser()

      const result = parser.feed(new Uint8Array(0))
      expect(result.state).toBe('awaiting_header')
      expect(result.bytesConsumed).toBe(0)
    })

  })

})
