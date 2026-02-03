/**
 * Index command handlers for MongoDB wire protocol
 * Handles: createIndexes, dropIndexes, listIndexes, create (collection)
 */

import type { ExtractedCommand } from '../message-parser.js';
import {
  buildSuccessResponse,
  buildErrorResponse,
} from '../bson-serializer.js';
import type { CommandContext, CommandResult, CommandHandler } from './types.js';

// ============================================================================
// Index Command Handlers
// ============================================================================

export async function handleCreate(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  if (!cmd.collection) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        2,
        'Collection name required for create',
        'InvalidNamespace'
      ),
    };
  }

  const db = ctx.client.db(cmd.database);
  await db.createCollection(cmd.collection);

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId),
  };
}

export async function handleCreateIndexes(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  if (!cmd.collection) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        2,
        'Collection name required for createIndexes',
        'InvalidNamespace'
      ),
    };
  }

  const db = ctx.client.db(cmd.database);
  const collection = db.collection(cmd.collection);

  const indexes = (cmd.body.indexes || []) as Array<{
    key: Record<string, 1 | -1>;
    name?: string;
  }>;

  for (const index of indexes) {
    await collection.createIndex(index.key, { name: index.name });
  }

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      numIndexesBefore: 1,
      numIndexesAfter: 1 + indexes.length,
      createdCollectionAutomatically: false,
    }),
  };
}

export async function handleDropIndexes(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  if (!cmd.collection) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        2,
        'Collection name required for dropIndexes',
        'InvalidNamespace'
      ),
    };
  }

  const db = ctx.client.db(cmd.database);
  const collection = db.collection(cmd.collection);

  const indexName = cmd.body.index as string | undefined;
  if (indexName && indexName !== '*') {
    await collection.dropIndex(indexName);
  }

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      nIndexesWas: 1,
    }),
  };
}

export async function handleListIndexes(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  if (!cmd.collection) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        2,
        'Collection name required for listIndexes',
        'InvalidNamespace'
      ),
    };
  }

  const db = ctx.client.db(cmd.database);
  const collection = db.collection(cmd.collection);

  const indexes = await collection.listIndexes();

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      cursor: {
        id: 0n,
        ns: `${cmd.database}.${cmd.collection}`,
        firstBatch: indexes.map((idx) => ({
          v: 2,
          key: idx.key,
          name: idx.name,
        })),
      },
    }),
  };
}

/**
 * Index command handlers registry
 */
export const indexHandlers: Record<string, CommandHandler> = {
  create: handleCreate,
  createIndexes: handleCreateIndexes,
  dropIndexes: handleDropIndexes,
  listIndexes: handleListIndexes,
};
