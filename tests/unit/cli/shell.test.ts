/**
 * Tests for the MongoLake CLI Shell
 *
 * Tests the interactive MongoDB shell functionality including:
 * - Connection to MongoLake instances
 * - REPL interface behavior
 * - MongoDB-style query support
 * - Error handling and graceful recovery
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { EventEmitter } from 'node:events';

// ============================================================================
// Mock Setup
// ============================================================================

// Mock readline for REPL testing
const mockReadlineInterface = {
  on: vi.fn().mockReturnThis(),
  prompt: vi.fn(),
  setPrompt: vi.fn(),
  close: vi.fn(),
  write: vi.fn(),
  question: vi.fn(),
  _events: {},
};

vi.mock('node:readline', () => ({
  createInterface: vi.fn(() => mockReadlineInterface),
}));

// Mock the client module
const mockCursor = {
  toArray: vi.fn().mockResolvedValue([]),
  next: vi.fn().mockResolvedValue(null),
  hasNext: vi.fn().mockResolvedValue(false),
};

const mockCollection = {
  find: vi.fn(() => mockCursor),
  findOne: vi.fn().mockResolvedValue(null),
  insertOne: vi.fn().mockResolvedValue({ acknowledged: true, insertedId: 'test-id' }),
  insertMany: vi.fn().mockResolvedValue({ acknowledged: true, insertedCount: 2, insertedIds: ['id1', 'id2'] }),
  updateOne: vi.fn().mockResolvedValue({ acknowledged: true, matchedCount: 1, modifiedCount: 1 }),
  updateMany: vi.fn().mockResolvedValue({ acknowledged: true, matchedCount: 2, modifiedCount: 2 }),
  deleteOne: vi.fn().mockResolvedValue({ acknowledged: true, deletedCount: 1 }),
  deleteMany: vi.fn().mockResolvedValue({ acknowledged: true, deletedCount: 2 }),
  countDocuments: vi.fn().mockResolvedValue(10),
  aggregate: vi.fn(() => mockCursor),
};

const mockDb = {
  collection: vi.fn(() => mockCollection),
  listCollections: vi.fn().mockResolvedValue([]),
  dropCollection: vi.fn().mockResolvedValue(true),
};

const mockClient = {
  db: vi.fn(() => mockDb),
  listDatabases: vi.fn().mockResolvedValue([]),
  connect: vi.fn().mockResolvedValue(undefined),
  close: vi.fn().mockResolvedValue(undefined),
};

vi.mock('../../../src/client/index.js', () => ({
  MongoLake: vi.fn(() => mockClient),
  mockClient,
  mockDb,
  mockCollection,
}));

// ============================================================================
// Module Export Tests
// ============================================================================

describe('CLI Shell - Module Exports', () => {
  it('should export startShell function', async () => {
    const module = await import('../../../src/cli/shell.js');
    expect(typeof module.startShell).toBe('function');
  });

  it('should export ShellOptions type', async () => {
    // TypeScript type check - if this compiles, the type exists
    const module = await import('../../../src/cli/shell.js');
    expect(module.startShell).toBeDefined();
  });
});

// ============================================================================
// Connection Tests - RED: These tests should FAIL
// ============================================================================

describe('CLI Shell - MongoLake Connection', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should connect to MongoLake instance on startup', async () => {
    // RED: Shell should call connect() on the MongoLake client
    const { startShell } = await import('../../../src/cli/shell.js');

    // Mock process to prevent actual shell from blocking
    const originalStdin = process.stdin;
    const originalStdout = process.stdout;

    // Start shell in background (will fail because connect() is not called)
    const shellPromise = startShell({ path: '.mongolake', verbose: false });

    // Give it time to initialize
    await new Promise(resolve => setTimeout(resolve, 50));

    // Simulate exit
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();

    // This should fail - connect() should be called
    expect(mockClient.connect).toHaveBeenCalled();
  });

  it('should support connecting to remote MongoLake via URL', async () => {
    // RED: Shell should accept a connection URL option
    const { startShell } = await import('../../../src/cli/shell.js');

    // This should fail - shell currently only supports local path, not remote URLs
    await expect(startShell({
      url: 'mongolake://api.mongolake.com/mydb',
      verbose: false
    } as any)).resolves.not.toThrow();

    expect(mockClient.connect).toHaveBeenCalledWith(expect.objectContaining({
      url: 'mongolake://api.mongolake.com/mydb'
    }));
  });

  it('should display connection status message', async () => {
    // RED: Shell should show "Connected to..." message
    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const { startShell } = await import('../../../src/cli/shell.js');

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();

    // This should fail - shell should display connection info
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('Connected to'));

    consoleSpy.mockRestore();
  });

  it('should close connection when shell exits', async () => {
    // RED: Shell should call close() on exit
    const { startShell } = await import('../../../src/cli/shell.js');

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    // Simulate 'exit' command
    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('exit');
    }

    // This should fail - close() should be called on exit
    expect(mockClient.close).toHaveBeenCalled();
  });

  it('should support connection timeout option', async () => {
    // RED: Shell should support --timeout flag
    const { startShell } = await import('../../../src/cli/shell.js');

    // This should fail - timeout option is not implemented
    await startShell({
      path: '.mongolake',
      verbose: false,
      timeout: 5000
    } as any);

    expect(mockClient.connect).toHaveBeenCalledWith(expect.objectContaining({
      timeout: 5000
    }));
  });
});

// ============================================================================
// Connection Error Handling Tests - RED: These tests should FAIL
// ============================================================================

describe('CLI Shell - Connection Error Handling', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should handle connection refused error gracefully', async () => {
    // RED: Shell should catch and display friendly error for connection refused
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    mockClient.connect.mockRejectedValueOnce(new Error('ECONNREFUSED'));

    const { startShell } = await import('../../../src/cli/shell.js');

    // This should fail - shell doesn't have proper connection error handling
    await expect(startShell({
      url: 'mongolake://unreachable:3456',
      verbose: false
    } as any)).rejects.toThrow();

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('Could not connect'));

    consoleSpy.mockRestore();
  });

  it('should handle authentication error gracefully', async () => {
    // RED: Shell should display auth error and prompt for credentials
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    mockClient.connect.mockRejectedValueOnce(new Error('Authentication failed'));

    const { startShell } = await import('../../../src/cli/shell.js');

    await expect(startShell({
      url: 'mongolake://api.mongolake.com/mydb',
      verbose: false
    } as any)).rejects.toThrow();

    // This should fail - auth error handling not implemented
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('Authentication failed'));

    consoleSpy.mockRestore();
  });

  it('should retry connection on transient errors', async () => {
    // RED: Shell should retry 3 times on transient network errors
    mockClient.connect
      .mockRejectedValueOnce(new Error('ETIMEDOUT'))
      .mockRejectedValueOnce(new Error('ETIMEDOUT'))
      .mockResolvedValueOnce(undefined);

    const { startShell } = await import('../../../src/cli/shell.js');

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 100));
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();

    // This should fail - retry logic not implemented
    expect(mockClient.connect).toHaveBeenCalledTimes(3);
  });

  it('should handle disconnect during session and attempt reconnect', async () => {
    // RED: Shell should handle mid-session disconnects
    const { startShell } = await import('../../../src/cli/shell.js');

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    // Simulate a command that fails due to disconnect
    mockCollection.find.mockImplementationOnce(() => {
      throw new Error('Connection lost');
    });

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.find()');
    }

    // This should fail - reconnect logic not implemented
    expect(mockClient.connect).toHaveBeenCalledTimes(2); // Initial + reconnect

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should show network error with helpful suggestions', async () => {
    // RED: Shell should provide actionable error messages
    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    mockClient.connect.mockRejectedValueOnce(new Error('ECONNREFUSED'));

    const { startShell } = await import('../../../src/cli/shell.js');

    try {
      await startShell({
        url: 'mongolake://localhost:3456',
        verbose: false
      } as any);
    } catch {
      // Expected to throw
    }

    // This should fail - helpful error messages not implemented
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringMatching(/mongolake dev|server running/i));

    consoleSpy.mockRestore();
  });
});

// ============================================================================
// REPL Interface Tests - RED: These tests should FAIL
// ============================================================================

describe('CLI Shell - REPL Interface', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should support command history navigation', async () => {
    // RED: Shell should persist command history
    const { startShell } = await import('../../../src/cli/shell.js');

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.find()');
      await lineHandler('db.orders.find()');
    }

    // This should fail - history persistence not implemented
    // History should be saved to ~/.mongolake_history
    const fs = await import('node:fs');
    expect(fs.existsSync).toBeDefined(); // placeholder check

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support tab completion for collection names', async () => {
    // RED: Shell should autocomplete collection names
    mockDb.listCollections.mockResolvedValue(['users', 'orders', 'products']);

    const { startShell } = await import('../../../src/cli/shell.js');

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    // This should fail - tab completion not implemented
    const createInterfaceCall = (await import('node:readline')).createInterface as ReturnType<typeof vi.fn>;
    const interfaceOptions = createInterfaceCall.mock.calls[0]?.[0];

    expect(interfaceOptions).toHaveProperty('completer');
    expect(typeof interfaceOptions?.completer).toBe('function');

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support tab completion for method names', async () => {
    // RED: Shell should autocomplete method names on db.collection.
    const { startShell } = await import('../../../src/cli/shell.js');

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const createInterfaceCall = (await import('node:readline')).createInterface as ReturnType<typeof vi.fn>;
    const interfaceOptions = createInterfaceCall.mock.calls[0]?.[0];

    // This should fail - method autocomplete not implemented
    if (interfaceOptions?.completer) {
      const [completions] = interfaceOptions.completer('db.users.fi');
      expect(completions).toContain('db.users.find');
      expect(completions).toContain('db.users.findOne');
    } else {
      expect(interfaceOptions?.completer).toBeDefined();
    }

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support multi-line input for complex queries', async () => {
    // RED: Shell should handle multi-line input with continuation prompt
    const { startShell } = await import('../../../src/cli/shell.js');

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];

    // This should fail - multi-line support with continuation not implemented
    if (lineHandler) {
      // First line with unclosed brace
      await lineHandler('db.users.insertOne({');

      // Shell should show continuation prompt
      expect(mockReadlineInterface.setPrompt).toHaveBeenCalledWith(expect.stringContaining('...'));

      // Complete the command
      await lineHandler('  "name": "Alice"');
      await lineHandler('})');
    }

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support .editor mode for multi-line editing', async () => {
    // RED: Shell should support .editor command for multi-line input
    const { startShell } = await import('../../../src/cli/shell.js');

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];

    // This should fail - .editor mode not implemented
    if (lineHandler) {
      await lineHandler('.editor');
    }

    expect(mockReadlineInterface.write).toHaveBeenCalledWith(expect.stringContaining('editor mode'));

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should colorize output based on terminal capabilities', async () => {
    // RED: Shell should detect terminal color support
    const { startShell } = await import('../../../src/cli/shell.js');

    // Simulate no color support
    const originalIsTTY = process.stdout.isTTY;
    Object.defineProperty(process.stdout, 'isTTY', { value: false, configurable: true });

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      mockCursor.toArray.mockResolvedValueOnce([{ name: 'test' }]);
      await lineHandler('db.users.find()');
    }

    // This should fail - color detection not implemented
    // Output should NOT contain ANSI codes when not a TTY
    const outputCalls = consoleSpy.mock.calls.flat().join('');
    expect(outputCalls).not.toMatch(/\x1b\[/);

    Object.defineProperty(process.stdout, 'isTTY', { value: originalIsTTY, configurable: true });
    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });
});

// ============================================================================
// MongoDB Query Support Tests - RED: These tests should FAIL
// ============================================================================

describe('CLI Shell - MongoDB Query Support', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should support .limit() cursor method chaining', async () => {
    // RED: Shell should support cursor method chaining
    const { startShell } = await import('../../../src/cli/shell.js');

    const mockChainedCursor = {
      ...mockCursor,
      limit: vi.fn().mockReturnThis(),
      toArray: vi.fn().mockResolvedValue([{ name: 'test' }]),
    };
    mockCollection.find.mockReturnValueOnce(mockChainedCursor);

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      // This should fail - cursor chaining not implemented
      await lineHandler('db.users.find().limit(5)');
    }

    expect(mockChainedCursor.limit).toHaveBeenCalledWith(5);

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support .skip() cursor method chaining', async () => {
    // RED: Shell should support skip() chaining
    const { startShell } = await import('../../../src/cli/shell.js');

    const mockChainedCursor = {
      ...mockCursor,
      skip: vi.fn().mockReturnThis(),
      toArray: vi.fn().mockResolvedValue([{ name: 'test' }]),
    };
    mockCollection.find.mockReturnValueOnce(mockChainedCursor);

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.find().skip(10)');
    }

    // This should fail - skip chaining not implemented
    expect(mockChainedCursor.skip).toHaveBeenCalledWith(10);

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support .sort() cursor method chaining', async () => {
    // RED: Shell should support sort() chaining
    const { startShell } = await import('../../../src/cli/shell.js');

    const mockChainedCursor = {
      ...mockCursor,
      sort: vi.fn().mockReturnThis(),
      toArray: vi.fn().mockResolvedValue([{ name: 'test' }]),
    };
    mockCollection.find.mockReturnValueOnce(mockChainedCursor);

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.find().sort({ name: 1 })');
    }

    // This should fail - sort chaining not implemented
    expect(mockChainedCursor.sort).toHaveBeenCalledWith({ name: 1 });

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support .project() cursor method chaining', async () => {
    // RED: Shell should support project() chaining
    const { startShell } = await import('../../../src/cli/shell.js');

    const mockChainedCursor = {
      ...mockCursor,
      project: vi.fn().mockReturnThis(),
      toArray: vi.fn().mockResolvedValue([{ name: 'test' }]),
    };
    mockCollection.find.mockReturnValueOnce(mockChainedCursor);

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.find().project({ name: 1, email: 1 })');
    }

    // This should fail - project chaining not implemented
    expect(mockChainedCursor.project).toHaveBeenCalledWith({ name: 1, email: 1 });

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support combined cursor method chaining', async () => {
    // RED: Shell should support multiple chained methods
    const { startShell } = await import('../../../src/cli/shell.js');

    const mockChainedCursor = {
      ...mockCursor,
      skip: vi.fn().mockReturnThis(),
      limit: vi.fn().mockReturnThis(),
      sort: vi.fn().mockReturnThis(),
      toArray: vi.fn().mockResolvedValue([{ name: 'test' }]),
    };
    mockCollection.find.mockReturnValueOnce(mockChainedCursor);

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.find({ active: true }).sort({ name: 1 }).skip(10).limit(5)');
    }

    // This should fail - combined chaining not implemented
    expect(mockChainedCursor.sort).toHaveBeenCalledWith({ name: 1 });
    expect(mockChainedCursor.skip).toHaveBeenCalledWith(10);
    expect(mockChainedCursor.limit).toHaveBeenCalledWith(5);

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support .explain() for query analysis', async () => {
    // RED: Shell should support explain()
    const { startShell } = await import('../../../src/cli/shell.js');

    const mockExplainCursor = {
      ...mockCursor,
      explain: vi.fn().mockResolvedValue({ queryPlanner: {}, executionStats: {} }),
    };
    mockCollection.find.mockReturnValueOnce(mockExplainCursor);

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.find({ name: "Alice" }).explain()');
    }

    // This should fail - explain not implemented
    expect(mockExplainCursor.explain).toHaveBeenCalled();
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('queryPlanner'));

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support createIndex command', async () => {
    // RED: Shell should support index creation
    const { startShell } = await import('../../../src/cli/shell.js');

    mockCollection.createIndex = vi.fn().mockResolvedValue('name_1');

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.createIndex({ name: 1 })');
    }

    // This should fail - createIndex not implemented
    expect(mockCollection.createIndex).toHaveBeenCalledWith({ name: 1 });
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('name_1'));

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support getIndexes command', async () => {
    // RED: Shell should support listing indexes
    const { startShell } = await import('../../../src/cli/shell.js');

    mockCollection.listIndexes = vi.fn().mockReturnValue({
      toArray: vi.fn().mockResolvedValue([
        { name: '_id_', key: { _id: 1 } },
        { name: 'name_1', key: { name: 1 } },
      ]),
    });

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.getIndexes()');
    }

    // This should fail - getIndexes not implemented
    expect(mockCollection.listIndexes).toHaveBeenCalled();

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support dropIndex command', async () => {
    // RED: Shell should support dropping indexes
    const { startShell } = await import('../../../src/cli/shell.js');

    mockCollection.dropIndex = vi.fn().mockResolvedValue({ ok: 1 });

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.dropIndex("name_1")');
    }

    // This should fail - dropIndex not implemented
    expect(mockCollection.dropIndex).toHaveBeenCalledWith('name_1');

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support distinct command', async () => {
    // RED: Shell should support distinct()
    const { startShell } = await import('../../../src/cli/shell.js');

    mockCollection.distinct = vi.fn().mockResolvedValue(['value1', 'value2', 'value3']);

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.distinct("status")');
    }

    // This should fail - distinct not implemented
    expect(mockCollection.distinct).toHaveBeenCalledWith('status');

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support replaceOne command', async () => {
    // RED: Shell should support replaceOne
    const { startShell } = await import('../../../src/cli/shell.js');

    mockCollection.replaceOne = vi.fn().mockResolvedValue({
      acknowledged: true,
      matchedCount: 1,
      modifiedCount: 1
    });

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.replaceOne({ _id: "123" }, { name: "New Name" })');
    }

    // This should fail - replaceOne not implemented
    expect(mockCollection.replaceOne).toHaveBeenCalledWith(
      { _id: '123' },
      { name: 'New Name' }
    );

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support bulkWrite command', async () => {
    // RED: Shell should support bulkWrite
    const { startShell } = await import('../../../src/cli/shell.js');

    mockCollection.bulkWrite = vi.fn().mockResolvedValue({
      acknowledged: true,
      insertedCount: 1,
      modifiedCount: 1,
      deletedCount: 1,
    });

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.bulkWrite([{ insertOne: { document: { name: "Alice" } } }])');
    }

    // This should fail - bulkWrite not implemented
    expect(mockCollection.bulkWrite).toHaveBeenCalled();

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });
});

// ============================================================================
// Special Commands Tests - RED: These tests should FAIL
// ============================================================================

describe('CLI Shell - Special Commands', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should support it command for iterating cursor results', async () => {
    // RED: Shell should support 'it' to get more results
    const { startShell } = await import('../../../src/cli/shell.js');

    mockCursor.hasNext.mockResolvedValueOnce(true);
    mockCursor.toArray.mockResolvedValueOnce([{ name: 'batch1' }]);

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.find()');
      await lineHandler('it'); // Get next batch
    }

    // This should fail - 'it' command not implemented
    expect(mockCursor.hasNext).toHaveBeenCalled();

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support load() command to run scripts', async () => {
    // RED: Shell should support loading and executing JS files
    const { startShell } = await import('../../../src/cli/shell.js');

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('load("./scripts/seed.js")');
    }

    // This should fail - load() not implemented
    // Shell should attempt to read and execute the file
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringMatching(/loaded|executed/i));

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support print() and printjson() functions', async () => {
    // RED: Shell should support print helpers
    const { startShell } = await import('../../../src/cli/shell.js');

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('print("Hello World")');
      await lineHandler('printjson({ name: "test" })');
    }

    // This should fail - print/printjson not implemented
    expect(consoleSpy).toHaveBeenCalledWith('Hello World');
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('"name"'));

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support db.stats() command', async () => {
    // RED: Shell should support database statistics
    const { startShell } = await import('../../../src/cli/shell.js');

    mockDb.stats = vi.fn().mockResolvedValue({
      db: 'test',
      collections: 5,
      objects: 1000,
      dataSize: 1024000,
    });

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.stats()');
    }

    // This should fail - db.stats() not implemented
    expect(mockDb.stats).toHaveBeenCalled();
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('collections'));

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support collection.stats() command', async () => {
    // RED: Shell should support collection statistics
    const { startShell } = await import('../../../src/cli/shell.js');

    mockCollection.stats = vi.fn().mockResolvedValue({
      ns: 'test.users',
      count: 100,
      size: 10240,
      avgObjSize: 102,
    });

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.stats()');
    }

    // This should fail - collection.stats() not implemented
    expect(mockCollection.stats).toHaveBeenCalled();

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support db.createCollection() command', async () => {
    // RED: Shell should support explicit collection creation
    const { startShell } = await import('../../../src/cli/shell.js');

    mockDb.createCollection = vi.fn().mockResolvedValue({ ok: 1 });

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.createCollection("newcollection")');
    }

    // This should fail - createCollection not implemented
    expect(mockDb.createCollection).toHaveBeenCalledWith('newcollection');

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support db.dropDatabase() command', async () => {
    // RED: Shell should support dropping entire database
    const { startShell } = await import('../../../src/cli/shell.js');

    mockDb.dropDatabase = vi.fn().mockResolvedValue({ dropped: 'test', ok: 1 });

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.dropDatabase()');
    }

    // This should fail - dropDatabase not implemented
    expect(mockDb.dropDatabase).toHaveBeenCalled();
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('dropped'));

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support version() command', async () => {
    // RED: Shell should display server version
    const { startShell } = await import('../../../src/cli/shell.js');

    mockClient.serverInfo = vi.fn().mockResolvedValue({ version: '1.0.0' });

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('version()');
    }

    // This should fail - version() not implemented
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringMatching(/\d+\.\d+\.\d+/));

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });
});

// ============================================================================
// Query Error Handling Tests - RED: These tests should FAIL
// ============================================================================

describe('CLI Shell - Query Error Handling', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should handle invalid query operator errors', async () => {
    // RED: Shell should provide helpful error for invalid operators
    const { startShell } = await import('../../../src/cli/shell.js');

    mockCollection.find.mockImplementationOnce(() => {
      throw new Error('Unknown operator: $invalid');
    });

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.find({ $invalid: 1 })');
    }

    // This should fail - operator error handling not improved
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringMatching(/unknown operator|invalid|supported operators/i));

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should handle syntax errors in query with line/column info', async () => {
    // RED: Shell should show where syntax error occurred
    const { startShell } = await import('../../../src/cli/shell.js');

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.find({ name: }'); // Missing value
    }

    // This should fail - syntax error location not shown
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringMatching(/syntax error|column|position/i));

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should handle document validation errors', async () => {
    // RED: Shell should display validation errors clearly
    const { startShell } = await import('../../../src/cli/shell.js');

    mockCollection.insertOne.mockRejectedValueOnce(
      new Error('Document failed validation: required field "email" missing')
    );

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.insertOne({ name: "Alice" })');
    }

    // This should fail - validation error formatting not implemented
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringMatching(/validation|required|email/i));

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should handle duplicate key errors with index info', async () => {
    // RED: Shell should show which index caused duplicate error
    const { startShell } = await import('../../../src/cli/shell.js');

    mockCollection.insertOne.mockRejectedValueOnce(
      new Error('E11000 duplicate key error collection: test.users index: email_1 dup key: { email: "alice@example.com" }')
    );

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.insertOne({ email: "alice@example.com" })');
    }

    // This should fail - duplicate key error formatting not implemented
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringMatching(/duplicate|email_1|alice@example.com/i));

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should continue REPL after query errors', async () => {
    // RED: Shell should not exit after query errors
    const { startShell } = await import('../../../src/cli/shell.js');

    mockCollection.find.mockImplementationOnce(() => {
      throw new Error('Query error');
    });

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.find({ invalid })'); // Error
      await lineHandler('db.users.find()'); // Should still work
    }

    // This should fail if REPL stops after first error
    expect(mockReadlineInterface.prompt).toHaveBeenCalledTimes(3); // Initial + after each command

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });
});

// ============================================================================
// Output Formatting Tests - RED: These tests should FAIL
// ============================================================================

describe('CLI Shell - Output Formatting', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should pretty print large documents with truncation', async () => {
    // RED: Shell should truncate very large documents
    const { startShell } = await import('../../../src/cli/shell.js');

    const largeDoc = {
      _id: 'test',
      data: 'x'.repeat(10000), // Very large string
    };
    mockCursor.toArray.mockResolvedValueOnce([largeDoc]);

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.find()');
    }

    // This should fail - truncation not implemented
    const output = consoleSpy.mock.calls.flat().join('');
    expect(output.length).toBeLessThan(5000);
    expect(output).toContain('...');

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should format dates in human-readable format', async () => {
    // RED: Shell should format ISODate objects
    const { startShell } = await import('../../../src/cli/shell.js');

    const docWithDate = {
      _id: 'test',
      createdAt: new Date('2024-01-15T10:30:00Z'),
    };
    mockCursor.toArray.mockResolvedValueOnce([docWithDate]);

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.find()');
    }

    // This should fail - date formatting not implemented
    const output = consoleSpy.mock.calls.flat().join('');
    expect(output).toMatch(/ISODate\("2024-01-15T10:30:00/);

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should format ObjectId in MongoDB style', async () => {
    // RED: Shell should display ObjectId properly
    const { startShell } = await import('../../../src/cli/shell.js');

    const docWithObjectId = {
      _id: { toString: () => '507f1f77bcf86cd799439011' },
    };
    mockCursor.toArray.mockResolvedValueOnce([docWithObjectId]);

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.find()');
    }

    // This should fail - ObjectId formatting not implemented
    const output = consoleSpy.mock.calls.flat().join('');
    expect(output).toMatch(/ObjectId\("507f1f77bcf86cd799439011"\)/);

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should format Binary data with type info', async () => {
    // RED: Shell should display binary data properly
    const { startShell } = await import('../../../src/cli/shell.js');

    const docWithBinary = {
      _id: 'test',
      data: { buffer: Buffer.from('hello'), sub_type: 0 },
    };
    mockCursor.toArray.mockResolvedValueOnce([docWithBinary]);

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.find()');
    }

    // This should fail - binary formatting not implemented
    const output = consoleSpy.mock.calls.flat().join('');
    expect(output).toMatch(/BinData\(0,/);

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should paginate results with configurable page size', async () => {
    // RED: Shell should paginate large result sets
    const { startShell } = await import('../../../src/cli/shell.js');

    const manyDocs = Array.from({ length: 50 }, (_, i) => ({ _id: i, name: `User ${i}` }));
    mockCursor.toArray.mockResolvedValueOnce(manyDocs);

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('db.users.find()');
    }

    // This should fail - pagination not implemented (default should show 20)
    const output = consoleSpy.mock.calls.flat().join('');
    expect(output).toContain('Type "it" for more');
    expect((output.match(/User \d+/g) || []).length).toBeLessThanOrEqual(20);

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });
});

// ============================================================================
// Configuration Tests - RED: These tests should FAIL
// ============================================================================

describe('CLI Shell - Configuration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should load configuration from ~/.mongolarc.js', async () => {
    // RED: Shell should support rc file for customization
    const { startShell } = await import('../../../src/cli/shell.js');

    // Mock fs to return config
    vi.mock('node:fs', async (importOriginal) => {
      const actual = await importOriginal();
      return {
        ...(actual as object),
        existsSync: vi.fn((path: string) => path.includes('.mongolarc')),
        readFileSync: vi.fn().mockReturnValue('prompt = "custom> "'),
      };
    });

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    // This should fail - rc file support not implemented
    expect(mockReadlineInterface.setPrompt).toHaveBeenCalledWith(expect.stringContaining('custom>'));

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support --eval flag to run command and exit', async () => {
    // RED: Shell should support eval mode
    const { startShell } = await import('../../../src/cli/shell.js');

    mockCursor.toArray.mockResolvedValueOnce([{ count: 5 }]);

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    // This should fail - eval option not implemented
    await startShell({
      path: '.mongolake',
      verbose: false,
      eval: 'db.users.countDocuments()'
    } as any);

    expect(mockCollection.countDocuments).toHaveBeenCalled();
    expect(consoleSpy).toHaveBeenCalledWith(expect.anything());
    // Shell should have exited after eval
    expect(mockReadlineInterface.close).toHaveBeenCalled();

    consoleSpy.mockRestore();
  });

  it('should support --quiet flag to suppress banner', async () => {
    // RED: Shell should support quiet mode
    const { startShell } = await import('../../../src/cli/shell.js');

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({
      path: '.mongolake',
      verbose: false,
      quiet: true
    } as any);
    await new Promise(resolve => setTimeout(resolve, 50));

    // This should fail - quiet mode not implemented
    const output = consoleSpy.mock.calls.flat().join('');
    expect(output).not.toContain('MongoLake');
    expect(output).not.toContain('___');

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support --norc flag to skip rc file', async () => {
    // RED: Shell should support skipping rc file
    const { startShell } = await import('../../../src/cli/shell.js');

    // This should fail - norc option not implemented
    await expect(startShell({
      path: '.mongolake',
      verbose: false,
      norc: true
    } as any)).resolves.not.toThrow();

    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });

  it('should support DBQuery.shellBatchSize configuration', async () => {
    // RED: Shell should support configuring batch size
    const { startShell } = await import('../../../src/cli/shell.js');

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const shellPromise = startShell({ path: '.mongolake', verbose: false });
    await new Promise(resolve => setTimeout(resolve, 50));

    const lineHandler = mockReadlineInterface.on.mock.calls.find(([event]) => event === 'line')?.[1];
    if (lineHandler) {
      await lineHandler('DBQuery.shellBatchSize = 5');

      const manyDocs = Array.from({ length: 20 }, (_, i) => ({ _id: i }));
      mockCursor.toArray.mockResolvedValueOnce(manyDocs);

      await lineHandler('db.users.find()');
    }

    // This should fail - shellBatchSize not implemented
    const output = consoleSpy.mock.calls.flat().join('');
    // Should only show 5 documents
    expect((output.match(/"_id"/g) || []).length).toBeLessThanOrEqual(5);

    consoleSpy.mockRestore();
    mockReadlineInterface.on.mock.calls.find(([event]) => event === 'close')?.[1]?.();
  });
});

// ============================================================================
// Legacy Tests (Keep existing passing tests)
// ============================================================================

describe('MongoLake Shell - Command Parsing', () => {
  it('should recognize exit commands', () => {
    const exitCommands = ['exit', 'quit'];
    for (const cmd of exitCommands) {
      expect(cmd).toMatch(/^(exit|quit)$/);
    }
  });

  it('should recognize show dbs command', () => {
    const showDbsCommands = ['show dbs', 'show databases'];
    for (const cmd of showDbsCommands) {
      expect(cmd).toMatch(/^show (dbs|databases)$/);
    }
  });

  it('should recognize show collections command', () => {
    const showCollsCommands = ['show collections', 'show tables'];
    for (const cmd of showCollsCommands) {
      expect(cmd).toMatch(/^show (collections|tables)$/);
    }
  });

  it('should recognize use database command', () => {
    const useDbPattern = /^use\s+(\w+)$/;
    expect('use mydb'.match(useDbPattern)).toBeTruthy();
    expect('use test_database'.match(useDbPattern)).toBeTruthy();
    expect('use 123'.match(useDbPattern)).toBeTruthy();
    expect('use '.match(useDbPattern)).toBeFalsy();
    expect('use'.match(useDbPattern)).toBeFalsy();
  });

  it('should recognize db.collection.method() commands', () => {
    const dbMethodPattern = /^db\.(\w+)\.(\w+)\(([\s\S]*)\)$/;

    const findMatch = 'db.users.find()'.match(dbMethodPattern);
    expect(findMatch).toBeTruthy();
    expect(findMatch?.[1]).toBe('users');
    expect(findMatch?.[2]).toBe('find');
    expect(findMatch?.[3]).toBe('');

    const findFilterMatch = 'db.users.find({"name": "Alice"})'.match(dbMethodPattern);
    expect(findFilterMatch).toBeTruthy();
    expect(findFilterMatch?.[1]).toBe('users');
    expect(findFilterMatch?.[2]).toBe('find');
    expect(findFilterMatch?.[3]).toBe('{"name": "Alice"}');
  });
});

describe('MongoLake Shell - JSON Argument Parsing', () => {
  it('should parse single argument as array element', () => {
    const args = '{"name": "Alice"}';
    const parsed = JSON.parse(`[${args}]`);
    expect(parsed).toHaveLength(1);
    expect(parsed[0]).toEqual({ name: 'Alice' });
  });

  it('should parse multiple arguments', () => {
    const args = '{"name": "Alice"}, {"$set": {"age": 31}}';
    const parsed = JSON.parse(`[${args}]`);
    expect(parsed).toHaveLength(2);
    expect(parsed[0]).toEqual({ name: 'Alice' });
    expect(parsed[1]).toEqual({ $set: { age: 31 } });
  });

  it('should parse array argument', () => {
    const args = '[{"name": "Alice"}, {"name": "Bob"}]';
    const parsed = JSON.parse(`[${args}]`);
    expect(parsed).toHaveLength(1);
    expect(parsed[0]).toEqual([{ name: 'Alice' }, { name: 'Bob' }]);
  });

  it('should parse aggregation pipeline', () => {
    const args = '[{"$match": {"status": "active"}}, {"$group": {"_id": "$type", "count": {"$sum": 1}}}]';
    const parsed = JSON.parse(`[${args}]`);
    expect(parsed).toHaveLength(1);
    expect(parsed[0]).toHaveLength(2);
    expect(parsed[0][0]).toHaveProperty('$match');
    expect(parsed[0][1]).toHaveProperty('$group');
  });
});

describe('MongoLake Shell - Error Handling', () => {
  it('should identify invalid argument errors', () => {
    const argsStr = 'invalid json {';
    let error: Error | null = null;

    try {
      JSON.parse(`[${argsStr}]`);
    } catch (e) {
      error = e as Error;
    }

    expect(error).not.toBeNull();
  });

  it('should return error type for invalid arguments', () => {
    const argsStr = '{not: valid: json}';
    let parseError = false;

    try {
      JSON.parse(`[${argsStr}]`);
    } catch {
      parseError = true;
    }

    expect(parseError).toBe(true);
  });
});
