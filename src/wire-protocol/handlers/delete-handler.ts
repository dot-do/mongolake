/**
 * Delete command handlers for MongoDB wire protocol
 * Handles: delete, drop, dropDatabase
 */

import type { ExtractedCommand } from '../message-parser.js';
import {
  buildSuccessResponse,
  buildErrorResponse,
} from '../bson-serializer.js';
import type { Filter, Document as MongoDocument } from '../../types.js';
import type { CommandContext, CommandResult, CommandHandler } from './types.js';

// ============================================================================
// Delete Command Handlers
// ============================================================================

export async function handleDelete(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  if (!cmd.collection) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        2,
        'Collection name required for delete',
        'InvalidNamespace'
      ),
    };
  }

  const db = ctx.client.db(cmd.database);
  const collection = db.collection(cmd.collection);

  // Deletes can come from body.deletes or from document sequence sections
  const deletes = (cmd.body.deletes || cmd.documents || []) as Array<{
    q: Filter<MongoDocument>;
    limit?: number;
  }>;

  let deletedCount = 0;

  for (const del of deletes) {
    const filter = del.q || {};
    const limit = del.limit || 0;

    let result;
    if (limit === 1) {
      result = await collection.deleteOne(filter);
    } else {
      result = await collection.deleteMany(filter);
    }

    deletedCount += result.deletedCount;
  }

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      n: deletedCount,
    }),
  };
}

export async function handleDrop(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  if (!cmd.collection) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        2,
        'Collection name required for drop',
        'InvalidNamespace'
      ),
    };
  }

  const db = ctx.client.db(cmd.database);
  const dropped = await db.dropCollection(cmd.collection);

  if (!dropped) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        26,
        `ns not found: ${cmd.database}.${cmd.collection}`,
        'NamespaceNotFound'
      ),
    };
  }

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      nIndexesWas: 1,
      ns: `${cmd.database}.${cmd.collection}`,
    }),
  };
}

export async function handleDropDatabase(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  await ctx.client.dropDatabase(cmd.database);

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      dropped: cmd.database,
    }),
  };
}

/**
 * Delete command handlers registry
 */
export const deleteHandlers: Record<string, CommandHandler> = {
  delete: handleDelete,
  drop: handleDrop,
  dropDatabase: handleDropDatabase,
};
