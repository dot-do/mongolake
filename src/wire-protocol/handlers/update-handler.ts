/**
 * Update command handlers for MongoDB wire protocol
 * Handles: update, findAndModify
 */

import type { ExtractedCommand } from '../message-parser.js';
import {
  buildSuccessResponse,
  buildErrorResponse,
} from '../bson-serializer.js';
import type { Filter, Update, Document as MongoDocument } from '../../types.js';
import type { CommandContext, CommandResult, CommandHandler, Document } from './types.js';

// ============================================================================
// Update Command Handlers
// ============================================================================

export async function handleUpdate(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  if (!cmd.collection) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        2,
        'Collection name required for update',
        'InvalidNamespace'
      ),
    };
  }

  const db = ctx.client.db(cmd.database);
  const collection = db.collection(cmd.collection);

  // Updates can come from body.updates or from document sequence sections
  const updates = (cmd.body.updates || cmd.documents || []) as Array<{
    q: Filter<MongoDocument>;
    u: Update<MongoDocument>;
    upsert?: boolean;
    multi?: boolean;
  }>;

  let matchedCount = 0;
  let modifiedCount = 0;
  let upsertedCount = 0;
  const upserted: Array<{ index: number; _id: unknown }> = [];

  for (let i = 0; i < updates.length; i++) {
    const update = updates[i]!;
    const filter = update.q || {};
    const updateDoc = update.u;
    const upsert = update.upsert || false;
    const multi = update.multi || false;

    let result;
    if (multi) {
      result = await collection.updateMany(filter, updateDoc as Update<MongoDocument>, { upsert });
    } else {
      result = await collection.updateOne(filter, updateDoc as Update<MongoDocument>, { upsert });
    }

    matchedCount += result.matchedCount;
    modifiedCount += result.modifiedCount;
    upsertedCount += result.upsertedCount;

    if (result.upsertedId) {
      upserted.push({ index: i, _id: result.upsertedId });
    }
  }

  const response: Document = {
    n: matchedCount,
    nModified: modifiedCount,
  };

  if (upserted.length > 0) {
    response.upserted = upserted;
  }

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, response),
  };
}

export async function handleFindAndModify(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  if (!cmd.collection) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        2,
        'Collection name required for findAndModify',
        'InvalidNamespace'
      ),
    };
  }

  const db = ctx.client.db(cmd.database);
  const collection = db.collection(cmd.collection);

  const filter = (cmd.body.query || {}) as Filter<MongoDocument>;
  const update = cmd.body.update as Update<MongoDocument> | undefined;
  const remove = cmd.body.remove as boolean | undefined;
  const upsert = cmd.body.upsert as boolean | undefined;
  const returnNew = cmd.body.new as boolean | undefined;

  // Get the document before modification
  const doc = await collection.findOne(filter);

  if (remove) {
    if (doc) {
      await collection.deleteOne(filter);
    }
    return {
      response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
        value: doc || null,
        lastErrorObject: {
          n: doc ? 1 : 0,
          updatedExisting: false,
        },
      }),
    };
  }

  if (update) {
    const result = await collection.updateOne(filter, update, { upsert });

    if (returnNew) {
      const newDoc = await collection.findOne(filter);
      return {
        response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
          value: newDoc || null,
          lastErrorObject: {
            n: result.matchedCount + result.upsertedCount,
            updatedExisting: result.matchedCount > 0,
            upserted: result.upsertedId,
          },
        }),
      };
    }

    return {
      response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
        value: doc || null,
        lastErrorObject: {
          n: result.matchedCount + result.upsertedCount,
          updatedExisting: result.matchedCount > 0,
          upserted: result.upsertedId,
        },
      }),
    };
  }

  return {
    response: buildErrorResponse(
      ctx.requestId + 1,
      ctx.requestId,
      2,
      'Either update or remove must be specified',
      'FailedToParse'
    ),
  };
}

/**
 * Update command handlers registry
 */
export const updateHandlers: Record<string, CommandHandler> = {
  update: handleUpdate,
  findAndModify: handleFindAndModify,
};
