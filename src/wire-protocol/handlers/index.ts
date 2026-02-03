/**
 * Command Registry for MongoDB Wire Protocol
 *
 * Exports a unified command registry that maps command names to their handlers.
 * All handlers implement the CommandHandler interface.
 */

import type { CommandHandler, CommandRegistry, CommandContext, CommandResult } from './types.js';

// Import handler registries from each module
import { adminHandlers } from './admin-handler.js';
import { findHandlers, getCursorStore } from './find-handler.js';
import { insertHandlers } from './insert-handler.js';
import { updateHandlers } from './update-handler.js';
import { deleteHandlers } from './delete-handler.js';
import { aggregateHandlers } from './aggregate-handler.js';
import { indexHandlers } from './index-handler.js';

/**
 * Combined command registry mapping all command names to their handlers
 */
export const commandRegistry: CommandRegistry = {
  // Admin commands
  ...adminHandlers,

  // Find/query commands
  ...findHandlers,

  // Insert commands
  ...insertHandlers,

  // Update commands
  ...updateHandlers,

  // Delete commands
  ...deleteHandlers,

  // Aggregation commands
  ...aggregateHandlers,

  // Index/collection commands
  ...indexHandlers,
};

/**
 * Get a handler for a specific command name
 */
export function getHandler(commandName: string): CommandHandler | undefined {
  return commandRegistry[commandName];
}

/**
 * Check if a command is supported
 */
export function isCommandSupported(commandName: string): boolean {
  return commandName in commandRegistry;
}

/**
 * Get all supported command names
 */
export function getSupportedCommands(): string[] {
  return Object.keys(commandRegistry);
}

// Re-export types
export type { CommandHandler, CommandRegistry, CommandContext, CommandResult };

// Re-export cursor store for backwards compatibility
export { getCursorStore };

// Re-export individual handlers for direct access if needed
export { adminHandlers } from './admin-handler.js';
export { findHandlers } from './find-handler.js';
export { insertHandlers } from './insert-handler.js';
export { updateHandlers } from './update-handler.js';
export { deleteHandlers } from './delete-handler.js';
export { aggregateHandlers } from './aggregate-handler.js';
export { indexHandlers } from './index-handler.js';

// Re-export individual handler functions for backwards compatibility
export {
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
} from './admin-handler.js';

export {
  handleFind,
  handleCount,
  handleDistinct,
  handleGetMore,
  handleKillCursors,
} from './find-handler.js';

export { handleInsert } from './insert-handler.js';

export {
  handleUpdate,
  handleFindAndModify,
} from './update-handler.js';

export {
  handleDelete,
  handleDrop,
  handleDropDatabase,
} from './delete-handler.js';

export { handleAggregate } from './aggregate-handler.js';

export {
  handleCreate,
  handleCreateIndexes,
  handleDropIndexes,
  handleListIndexes,
} from './index-handler.js';
