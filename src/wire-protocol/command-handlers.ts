/**
 * MongoDB Wire Protocol Command Handlers
 *
 * Routes wire protocol commands to MongoLake storage layer.
 * Handles: find, insert, update, delete, and admin commands.
 *
 * This module now uses a registry pattern for command routing.
 * Individual handlers are organized in the handlers/ directory:
 * - admin-handler.ts: ping, hello, isMaster, buildInfo, etc.
 * - find-handler.ts: find, count, distinct, getMore, killCursors
 * - insert-handler.ts: insert
 * - update-handler.ts: update, findAndModify
 * - delete-handler.ts: delete, drop, dropDatabase
 * - aggregate-handler.ts: aggregate
 * - index-handler.ts: create, createIndexes, dropIndexes, listIndexes
 */

import type { ExtractedCommand } from './message-parser.js';
import { buildErrorResponse } from './bson-serializer.js';
import { logger } from '../utils/logger.js';
import {
  commandRegistry,
  getHandler,
  getCursorStore,
  type CommandContext,
  type CommandResult,
  type CommandHandler,
} from './handlers/index.js';

// Re-export types for backwards compatibility
export type { CommandContext, CommandResult };

// Re-export getCursorStore for backwards compatibility
export { getCursorStore };

/**
 * Execute a command and return the response
 */
export async function executeCommand(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  const handler = getHandler(cmd.name);

  if (!handler) {
    logger.warn('Unknown command received', {
      command: cmd.name,
      connectionId: ctx.connectionId,
    });
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        59,
        `no such command: '${cmd.name}'`,
        'CommandNotFound'
      ),
    };
  }

  try {
    return await handler(cmd, ctx);
  } catch (error) {
    logger.error('Error executing command', {
      command: cmd.name,
      connectionId: ctx.connectionId,
      error: error instanceof Error ? error : String(error),
      stack: error instanceof Error ? error.stack : undefined,
    });
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        1,
        error instanceof Error ? error.message : String(error),
        'InternalError'
      ),
    };
  }
}

// Re-export the command registry for advanced use cases
export { commandRegistry, getHandler };

// Re-export CommandHandler type
export type { CommandHandler };

// Re-export all individual handlers for backwards compatibility
export {
  // Admin handlers
  handlePing,
  handleHello,
  handleIsMaster,
  handleBuildInfo,
  handleServerStatus,
  handleListDatabases,
  handleListCollections,
  handleWhatsMyUri,
  handleGetLog,
  handleHostInfo,
  handleGetCmdLineOpts,
  handleGetParameter,
  handleGetFreeMonitoringStatus,
  handleSaslStart,
  handleSaslContinue,
  handleEndSessions,
  // Find handlers
  handleFind,
  handleCount,
  handleDistinct,
  handleGetMore,
  handleKillCursors,
  // Insert handlers
  handleInsert,
  // Update handlers
  handleUpdate,
  handleFindAndModify,
  // Delete handlers
  handleDelete,
  handleDrop,
  handleDropDatabase,
  // Aggregate handlers
  handleAggregate,
  // Index handlers
  handleCreate,
  handleCreateIndexes,
  handleDropIndexes,
  handleListIndexes,
} from './handlers/index.js';
