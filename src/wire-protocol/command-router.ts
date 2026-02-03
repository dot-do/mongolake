/**
 * MongoDB Wire Protocol Command Router
 *
 * Routes wire protocol commands to appropriate handlers.
 * Provides a clean interface between parsed OP_MSG messages
 * and command execution.
 */

import type { OpMsgMessage, Document } from './message-parser.js';
import { extractCommand } from './message-parser.js';
import {
  decodeCommand,
  CommandValidationError,
  type DecodedCommand,
} from './command-decoder.js';
import type { Document as MongoDocument } from '../types.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Parsed command structure passed to handlers
 */
export interface ParsedCommand {
  /** Command name (e.g., 'find', 'insert', 'ping') */
  name: string;
  /** Target database */
  database: string;
  /** Collection name (for CRUD commands) */
  collection?: string;
  /** Full command body */
  body: Document;
  /** Documents from document sequence sections */
  documents?: Document[];
  /** Decoded command with full type information */
  decoded?: DecodedCommand;
}

/**
 * Command handler interface
 */
export interface CommandHandler {
  /**
   * Execute the command
   *
   * @param command - Parsed command with full context
   * @returns Response document
   */
  execute(command: ParsedCommand): Promise<Document>;
}

/**
 * Map of command names to handlers
 */
export type CommandHandlers = Record<string, CommandHandler>;

/**
 * Command response with standard MongoDB format
 */
export interface CommandResponse {
  ok: 0 | 1;
  errmsg?: string;
  code?: number;
  codeName?: string;
  [key: string]: unknown;
}

// ============================================================================
// Error Codes
// ============================================================================

const ErrorCodes = {
  OK: 0,
  InternalError: 1,
  BadValue: 2,
  NoSuchKey: 4,
  GraphContainsCycle: 5,
  HostUnreachable: 6,
  HostNotFound: 7,
  UnknownError: 8,
  FailedToParse: 9,
  CannotMutateObject: 10,
  UserNotFound: 11,
  UnsupportedFormat: 12,
  Unauthorized: 13,
  TypeMismatch: 14,
  InvalidLength: 21,
  InvalidBSON: 22,
  CursorNotFound: 43,
  CommandNotFound: 59,
  NamespaceNotFound: 26,
} as const;

// ============================================================================
// Command Router
// ============================================================================

/**
 * Routes MongoDB wire protocol commands to appropriate handlers.
 *
 * @example
 * ```typescript
 * const handlers: CommandHandlers = {
 *   find: { execute: async (cmd) => ({ ok: 1, cursor: { ... } }) },
 *   insert: { execute: async (cmd) => ({ ok: 1, n: 1 }) },
 *   ping: { execute: async () => ({ ok: 1 }) },
 * };
 *
 * const router = new CommandRouter(handlers);
 * const response = await router.route(parsedOpMsg);
 * ```
 */
export class CommandRouter {
  private handlers: Map<string, CommandHandler>;
  private aliases: Map<string, string>;

  /**
   * Create a new command router
   *
   * @param handlers - Map of command names to handlers
   */
  constructor(handlers: CommandHandlers = {}) {
    this.handlers = new Map(Object.entries(handlers));
    this.aliases = new Map();
  }

  /**
   * Route a parsed OP_MSG to the appropriate handler
   *
   * @param message - Parsed OP_MSG
   * @returns Command response
   */
  async route(message: OpMsgMessage): Promise<CommandResponse> {
    try {
      // Extract command from message
      const extracted = extractCommand(message);

      // Parse to typed command for validation
      let decoded: DecodedCommand | undefined;
      try {
        decoded = decodeCommand(extracted.body as MongoDocument, extracted.documents as MongoDocument[] | undefined);
      } catch (e) {
        if (e instanceof CommandValidationError) {
          return {
            ok: 0,
            errmsg: e.message,
            code: e.code,
            codeName: e.codeName,
          };
        }
        throw e;
      }

      // Build parsed command
      const command: ParsedCommand = {
        name: extracted.name,
        database: extracted.database,
        collection: extracted.collection,
        body: extracted.body,
        documents: extracted.documents,
        decoded,
      };

      // Find handler
      const handler = this.getHandler(command.name);
      if (!handler) {
        return {
          ok: 0,
          errmsg: `no such command: '${command.name}'`,
          code: ErrorCodes.CommandNotFound,
          codeName: 'CommandNotFound',
        };
      }

      // Execute handler
      const result = await handler.execute(command);
      return {
        ok: 1,
        ...result,
      };
    } catch (error) {
      // Handle validation errors
      if (error instanceof CommandValidationError) {
        return {
          ok: 0,
          errmsg: error.message,
          code: error.code,
          codeName: error.codeName,
        };
      }

      // Handle other errors
      const message =
        error instanceof Error ? error.message : String(error);
      return {
        ok: 0,
        errmsg: message,
        code: ErrorCodes.InternalError,
        codeName: 'InternalError',
      };
    }
  }

  /**
   * Get handler for a command, checking aliases
   */
  private getHandler(name: string): CommandHandler | undefined {
    // Check direct handler
    let handler = this.handlers.get(name);
    if (handler) return handler;

    // Check alias
    const aliasTarget = this.aliases.get(name);
    if (aliasTarget) {
      handler = this.handlers.get(aliasTarget);
      if (handler) return handler;
    }

    return undefined;
  }

  /**
   * Register a command handler
   *
   * @param name - Command name
   * @param handler - Handler implementation
   */
  registerHandler(name: string, handler: CommandHandler): void {
    this.handlers.set(name, handler);
  }

  /**
   * Register a command alias
   *
   * @param alias - Alias name
   * @param target - Target command name
   */
  registerAlias(alias: string, target: string): void {
    this.aliases.set(alias, target);
  }

  /**
   * Check if a handler exists for a command
   *
   * @param name - Command name
   * @returns True if handler exists
   */
  hasHandler(name: string): boolean {
    return this.handlers.has(name) || this.aliases.has(name);
  }

  /**
   * Get list of registered command names
   *
   * @returns Array of command names
   */
  getRegisteredCommands(): string[] {
    return [...this.handlers.keys(), ...this.aliases.keys()];
  }

  /**
   * Unregister a command handler
   *
   * @param name - Command name
   * @returns True if handler was removed
   */
  unregisterHandler(name: string): boolean {
    return this.handlers.delete(name);
  }

  /**
   * Clear all handlers
   */
  clearHandlers(): void {
    this.handlers.clear();
    this.aliases.clear();
  }
}

// ============================================================================
// Factory Function
// ============================================================================

/**
 * Create a command router with default handlers
 */
export function createCommandRouter(handlers?: CommandHandlers): CommandRouter {
  return new CommandRouter(handlers);
}
