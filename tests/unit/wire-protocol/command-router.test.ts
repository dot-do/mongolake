/**
 * Command Router Tests
 *
 * Tests for the MongoDB wire protocol command router.
 * Following TDD approach - these tests are written FIRST before implementation.
 *
 * The CommandRouter is responsible for:
 * 1. Parsing incoming OP_MSG messages to extract commands
 * 2. Routing commands to appropriate handlers
 * 3. Returning properly formatted responses
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { CommandRouter, type CommandHandler, type CommandHandlers, type ParsedCommand } from '../../../src/wire-protocol/command-router.js';
import { parseOpMsg, extractCommand, type OpMsgMessage, type Document } from '../../../src/wire-protocol/message-parser.js';
import { serializeDocument } from '../../../src/wire-protocol/bson-serializer.js';
import { OpCode } from '../../../src/wire-protocol/message-parser.js';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Helper to create a BSON document for testing
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
      sectionsSize += 1 + section.payload.length;
    } else {
      const idBytes = new TextEncoder().encode(section.identifier! + '\0');
      sectionsSize += 1 + 4 + idBytes.length + section.payload.length;
    }
  }

  const messageLength = 16 + 4 + sectionsSize;
  const buffer = new Uint8Array(messageLength);
  const view = new DataView(buffer.buffer);

  // Header
  view.setInt32(0, messageLength, true);
  view.setInt32(4, requestId, true);
  view.setInt32(8, responseTo, true);
  view.setInt32(12, OpCode.OP_MSG, true);

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
 * Create a mock command handler
 */
function createMockHandler(): CommandHandler {
  return {
    execute: vi.fn().mockResolvedValue({
      ok: 1,
      result: 'success',
    }),
  };
}

/**
 * Create a simple OP_MSG for a command
 */
function createCommandMessage(
  command: Record<string, unknown>,
  database: string = 'test'
): Uint8Array {
  const doc = createBsonDocument({
    ...command,
    $db: database,
  });
  return createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);
}

// ============================================================================
// Tests
// ============================================================================

describe('CommandRouter', () => {
  let router: CommandRouter;
  let handlers: CommandHandlers;
  let findHandler: CommandHandler;
  let insertHandler: CommandHandler;
  let updateHandler: CommandHandler;
  let deleteHandler: CommandHandler;
  let aggregateHandler: CommandHandler;
  let getMoreHandler: CommandHandler;
  let pingHandler: CommandHandler;
  let helloHandler: CommandHandler;
  let isMasterHandler: CommandHandler;

  beforeEach(() => {
    // Create mock handlers
    findHandler = createMockHandler();
    insertHandler = createMockHandler();
    updateHandler = createMockHandler();
    deleteHandler = createMockHandler();
    aggregateHandler = createMockHandler();
    getMoreHandler = createMockHandler();
    pingHandler = createMockHandler();
    helloHandler = createMockHandler();
    isMasterHandler = createMockHandler();

    handlers = {
      find: findHandler,
      insert: insertHandler,
      update: updateHandler,
      delete: deleteHandler,
      aggregate: aggregateHandler,
      getMore: getMoreHandler,
      ping: pingHandler,
      hello: helloHandler,
      isMaster: isMasterHandler,
      ismaster: isMasterHandler, // Alias
    };

    router = new CommandRouter(handlers);
  });

  // ==========================================================================
  // Basic Routing Tests
  // ==========================================================================

  describe('Basic Command Routing', () => {
    it('should route find command to find handler', async () => {
      const message = createCommandMessage({ find: 'users', filter: {} });
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      expect(findHandler.execute).toHaveBeenCalled();
      const call = (findHandler.execute as ReturnType<typeof vi.fn>).mock.calls[0];
      expect(call[0].name).toBe('find');
      expect(call[0].collection).toBe('users');
    });

    it('should route insert command to insert handler', async () => {
      const message = createCommandMessage({
        insert: 'products',
        documents: [{ name: 'widget' }],
      });
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      expect(insertHandler.execute).toHaveBeenCalled();
      const call = (insertHandler.execute as ReturnType<typeof vi.fn>).mock.calls[0];
      expect(call[0].name).toBe('insert');
      expect(call[0].collection).toBe('products');
    });

    it('should route update command to update handler', async () => {
      const message = createCommandMessage({
        update: 'users',
        updates: [{ q: {}, u: { $set: { status: 'active' } } }],
      });
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      expect(updateHandler.execute).toHaveBeenCalled();
      const call = (updateHandler.execute as ReturnType<typeof vi.fn>).mock.calls[0];
      expect(call[0].name).toBe('update');
      expect(call[0].collection).toBe('users');
    });

    it('should route delete command to delete handler', async () => {
      const message = createCommandMessage({
        delete: 'sessions',
        deletes: [{ q: { expired: true }, limit: 0 }],
      });
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      expect(deleteHandler.execute).toHaveBeenCalled();
      const call = (deleteHandler.execute as ReturnType<typeof vi.fn>).mock.calls[0];
      expect(call[0].name).toBe('delete');
      expect(call[0].collection).toBe('sessions');
    });

    it('should route aggregate command to aggregate handler', async () => {
      const message = createCommandMessage({
        aggregate: 'orders',
        pipeline: [{ $match: { status: 'completed' } }],
        cursor: {},
      });
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      expect(aggregateHandler.execute).toHaveBeenCalled();
      const call = (aggregateHandler.execute as ReturnType<typeof vi.fn>).mock.calls[0];
      expect(call[0].name).toBe('aggregate');
      expect(call[0].collection).toBe('orders');
    });

    it('should route getMore command to cursor handler', async () => {
      const message = createCommandMessage({
        getMore: 12345n,
        collection: 'users',
      });
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      expect(getMoreHandler.execute).toHaveBeenCalled();
      const call = (getMoreHandler.execute as ReturnType<typeof vi.fn>).mock.calls[0];
      expect(call[0].name).toBe('getMore');
    });
  });

  // ==========================================================================
  // Admin Command Tests
  // ==========================================================================

  describe('Admin Commands', () => {
    it('should route ping command', async () => {
      const message = createCommandMessage({ ping: 1 }, 'admin');
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      expect(pingHandler.execute).toHaveBeenCalled();
      const call = (pingHandler.execute as ReturnType<typeof vi.fn>).mock.calls[0];
      expect(call[0].name).toBe('ping');
    });

    it('should route hello command', async () => {
      const message = createCommandMessage({ hello: 1 }, 'admin');
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      expect(helloHandler.execute).toHaveBeenCalled();
      const call = (helloHandler.execute as ReturnType<typeof vi.fn>).mock.calls[0];
      expect(call[0].name).toBe('hello');
    });

    it('should route isMaster command', async () => {
      const message = createCommandMessage({ isMaster: 1 }, 'admin');
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      expect(isMasterHandler.execute).toHaveBeenCalled();
    });

    it('should route ismaster command (lowercase alias)', async () => {
      const message = createCommandMessage({ ismaster: 1 }, 'admin');
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      expect(isMasterHandler.execute).toHaveBeenCalled();
    });
  });

  // ==========================================================================
  // Error Handling Tests
  // ==========================================================================

  describe('Error Handling', () => {
    it('should return error for unknown commands', async () => {
      const message = createCommandMessage({ unknownCommand: 1 });
      const parsed = parseOpMsg(message);

      const response = await router.route(parsed);

      expect(response.ok).toBe(0);
      expect(response.errmsg).toContain('no such command');
      expect(response.code).toBe(59); // CommandNotFound
    });

    it('should return error with command name in message', async () => {
      const message = createCommandMessage({ myCustomCommand: 1 });
      const parsed = parseOpMsg(message);

      const response = await router.route(parsed);

      expect(response.errmsg).toContain('myCustomCommand');
    });

    it('should handle handler errors gracefully', async () => {
      const errorHandler: CommandHandler = {
        execute: vi.fn().mockRejectedValue(new Error('Handler failed')),
      };
      const errorRouter = new CommandRouter({ errorCmd: errorHandler });

      const message = createCommandMessage({ errorCmd: 1 });
      const parsed = parseOpMsg(message);

      const response = await errorRouter.route(parsed);

      expect(response.ok).toBe(0);
      expect(response.errmsg).toContain('Handler failed');
    });
  });

  // ==========================================================================
  // Command Validation Tests
  // ==========================================================================

  describe('Command Validation', () => {
    it('should validate command structure', async () => {
      // Create a message with no command (only special fields)
      const doc = createBsonDocument({
        $db: 'test',
        $readPreference: { mode: 'primary' },
      });
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);
      const parsed = parseOpMsg(message);

      const response = await router.route(parsed);

      expect(response.ok).toBe(0);
      expect(response.errmsg).toBeDefined();
    });

    it('should validate collection name is not empty for CRUD commands', async () => {
      const message = createCommandMessage({ find: '', filter: {} });
      const parsed = parseOpMsg(message);

      const response = await router.route(parsed);

      expect(response.ok).toBe(0);
      expect(response.errmsg).toMatch(/collection/i);
    });

    it('should require $db field', async () => {
      // Create a message without $db
      const doc = createBsonDocument({ find: 'users' });
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);
      const parsed = parseOpMsg(message);

      const response = await router.route(parsed);

      expect(response.ok).toBe(0);
      expect(response.errmsg).toMatch(/\$db/i);
    });
  });

  // ==========================================================================
  // Database Extraction Tests
  // ==========================================================================

  describe('Database Extraction', () => {
    it('should extract database from $db field', async () => {
      const message = createCommandMessage({ find: 'users' }, 'myDatabase');
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      const call = (findHandler.execute as ReturnType<typeof vi.fn>).mock.calls[0];
      expect(call[0].database).toBe('myDatabase');
    });

    it('should pass database to handler context', async () => {
      const message = createCommandMessage({ ping: 1 }, 'admin');
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      const call = (pingHandler.execute as ReturnType<typeof vi.fn>).mock.calls[0];
      expect(call[0].database).toBe('admin');
    });
  });

  // ==========================================================================
  // Command Alias Tests
  // ==========================================================================

  describe('Command Aliases', () => {
    it('should handle command aliases (ismaster vs isMaster)', async () => {
      // Test lowercase
      const msg1 = createCommandMessage({ ismaster: 1 }, 'admin');
      await router.route(parseOpMsg(msg1));
      expect(isMasterHandler.execute).toHaveBeenCalledTimes(1);

      // Test camelCase
      const msg2 = createCommandMessage({ isMaster: 1 }, 'admin');
      await router.route(parseOpMsg(msg2));
      expect(isMasterHandler.execute).toHaveBeenCalledTimes(2);
    });
  });

  // ==========================================================================
  // ParsedCommand Structure Tests
  // ==========================================================================

  describe('ParsedCommand Structure', () => {
    it('should provide command name to handler', async () => {
      const message = createCommandMessage({ find: 'users' });
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      const call = (findHandler.execute as ReturnType<typeof vi.fn>).mock.calls[0];
      const cmd: ParsedCommand = call[0];
      expect(cmd.name).toBe('find');
    });

    it('should provide collection name for CRUD commands', async () => {
      const message = createCommandMessage({ insert: 'products', documents: [] });
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      const call = (insertHandler.execute as ReturnType<typeof vi.fn>).mock.calls[0];
      const cmd: ParsedCommand = call[0];
      expect(cmd.collection).toBe('products');
    });

    it('should provide full command body', async () => {
      const message = createCommandMessage({
        find: 'users',
        filter: { status: 'active' },
        limit: 10,
      });
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      const call = (findHandler.execute as ReturnType<typeof vi.fn>).mock.calls[0];
      const cmd: ParsedCommand = call[0];
      expect(cmd.body.filter).toEqual({ status: 'active' });
      expect(cmd.body.limit).toBe(10);
    });

    it('should include documents from document sequence sections', async () => {
      const bodyDoc = createBsonDocument({
        insert: 'products',
        $db: 'test',
      });
      const doc1 = createBsonDocument({ name: 'widget', price: 100 });
      const doc2 = createBsonDocument({ name: 'gadget', price: 200 });

      // Combine documents for type 1 section
      const docsPayload = new Uint8Array(doc1.length + doc2.length);
      docsPayload.set(doc1, 0);
      docsPayload.set(doc2, doc1.length);

      const message = createOpMsg(1, 0, 0, [
        { type: 0, payload: bodyDoc },
        { type: 1, payload: docsPayload, identifier: 'documents' },
      ]);
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      const call = (insertHandler.execute as ReturnType<typeof vi.fn>).mock.calls[0];
      const cmd: ParsedCommand = call[0];
      expect(cmd.documents).toBeDefined();
      expect(cmd.documents!.length).toBe(2);
    });
  });

  // ==========================================================================
  // Response Format Tests
  // ==========================================================================

  describe('Response Format', () => {
    it('should return handler response for successful commands', async () => {
      const mockResponse = { ok: 1, cursor: { firstBatch: [], id: 0n, ns: 'test.users' } };
      (findHandler.execute as ReturnType<typeof vi.fn>).mockResolvedValue(mockResponse);

      const message = createCommandMessage({ find: 'users' });
      const parsed = parseOpMsg(message);

      const response = await router.route(parsed);

      expect(response).toEqual(mockResponse);
    });

    it('should return proper error format for failures', async () => {
      const message = createCommandMessage({ unknownCmd: 1 });
      const parsed = parseOpMsg(message);

      const response = await router.route(parsed);

      expect(response).toHaveProperty('ok', 0);
      expect(response).toHaveProperty('errmsg');
      expect(response).toHaveProperty('code');
      expect(response).toHaveProperty('codeName');
    });
  });

  // ==========================================================================
  // Handler Registration Tests
  // ==========================================================================

  describe('Handler Registration', () => {
    it('should allow registering new handlers', () => {
      const customHandler = createMockHandler();
      router.registerHandler('customCommand', customHandler);

      expect(router.hasHandler('customCommand')).toBe(true);
    });

    it('should allow registering handler aliases', () => {
      const handler = createMockHandler();
      router.registerHandler('primaryCommand', handler);
      router.registerAlias('aliasCommand', 'primaryCommand');

      expect(router.hasHandler('aliasCommand')).toBe(true);
    });

    it('should list all registered command names', () => {
      const commands = router.getRegisteredCommands();

      expect(commands).toContain('find');
      expect(commands).toContain('insert');
      expect(commands).toContain('ping');
    });
  });

  // ==========================================================================
  // Edge Cases
  // ==========================================================================

  describe('Edge Cases', () => {
    it('should handle very long collection names', async () => {
      const longName = 'a'.repeat(200);
      const message = createCommandMessage({ find: longName });
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      const call = (findHandler.execute as ReturnType<typeof vi.fn>).mock.calls[0];
      expect(call[0].collection).toBe(longName);
    });

    it('should handle special characters in database name', async () => {
      const message = createCommandMessage({ ping: 1 }, 'my-database_123');
      const parsed = parseOpMsg(message);

      await router.route(parsed);

      const call = (pingHandler.execute as ReturnType<typeof vi.fn>).mock.calls[0];
      expect(call[0].database).toBe('my-database_123');
    });

    it('should handle multiple commands in sequence', async () => {
      const msg1 = createCommandMessage({ find: 'col1' });
      const msg2 = createCommandMessage({ find: 'col2' });
      const msg3 = createCommandMessage({ find: 'col3' });

      await router.route(parseOpMsg(msg1));
      await router.route(parseOpMsg(msg2));
      await router.route(parseOpMsg(msg3));

      expect(findHandler.execute).toHaveBeenCalledTimes(3);
    });
  });

  // ==========================================================================
  // Concurrent Request Tests
  // ==========================================================================

  describe('Concurrent Requests', () => {
    it('should handle concurrent requests correctly', async () => {
      const messages = Array.from({ length: 10 }, (_, i) =>
        createCommandMessage({ find: `collection${i}` })
      );

      const promises = messages.map(msg => router.route(parseOpMsg(msg)));
      await Promise.all(promises);

      expect(findHandler.execute).toHaveBeenCalledTimes(10);
    });
  });
});
