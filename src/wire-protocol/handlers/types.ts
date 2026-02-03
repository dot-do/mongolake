/**
 * Common types for wire protocol command handlers
 */

import type { ExtractedCommand, Document } from '../message-parser.js';
import type { MongoLake } from '../../client/index.js';

/**
 * Context passed to every command handler
 */
export interface CommandContext {
  client: MongoLake;
  requestId: number;
  connectionId: number;
}

/**
 * Result returned from command handlers
 */
export interface CommandResult {
  response: Uint8Array;
}

/**
 * Interface that all command handlers must implement
 */
export type CommandHandler = (
  cmd: ExtractedCommand,
  ctx: CommandContext
) => Promise<CommandResult>;

/**
 * Registry mapping command names to their handlers
 */
export type CommandRegistry = Record<string, CommandHandler>;

/**
 * Re-export Document type for convenience
 */
export type { Document };
