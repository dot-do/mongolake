/**
 * Insert command handler for MongoDB wire protocol
 * Handles: insert
 */

import type { ExtractedCommand } from '../message-parser.js';
import {
  buildSuccessResponse,
  buildErrorResponse,
} from '../bson-serializer.js';
import type { Document as MongoDocument } from '../../types.js';
import type { CommandContext, CommandResult, CommandHandler } from './types.js';

// ============================================================================
// Insert Command Handler
// ============================================================================

export async function handleInsert(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  if (!cmd.collection) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        2,
        'Collection name required for insert',
        'InvalidNamespace'
      ),
    };
  }

  const db = ctx.client.db(cmd.database);
  const collection = db.collection(cmd.collection);

  // Documents can come from body.documents or from document sequence sections
  const documents = (cmd.body.documents || cmd.documents || []) as MongoDocument[];

  if (documents.length === 0) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        2,
        'No documents to insert',
        'InvalidLength'
      ),
    };
  }

  const result = await collection.insertMany(documents);

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      n: result.insertedCount,
    }),
  };
}

/**
 * Insert command handlers registry
 */
export const insertHandlers: Record<string, CommandHandler> = {
  insert: handleInsert,
};
