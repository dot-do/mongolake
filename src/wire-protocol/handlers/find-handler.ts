/**
 * Find command handlers for MongoDB wire protocol
 * Handles: find, count, distinct, getMore, killCursors
 */

import type { ExtractedCommand } from '../message-parser.js';
import {
  buildSuccessResponse,
  buildErrorResponse,
} from '../bson-serializer.js';
import { FindCursor } from '../../client/index.js';
import { CursorStore } from '../../cursor/index.js';
import type { Filter, Document as MongoDocument } from '../../types.js';
import {
  WIRE_PROTOCOL_CURSOR_TIMEOUT_MS,
  WIRE_PROTOCOL_CURSOR_CLEANUP_INTERVAL_MS,
} from '../../constants.js';
import type { CommandContext, CommandResult, CommandHandler } from './types.js';

// Global cursor store for managing active cursors across connections
const cursorStore = new CursorStore({
  timeoutMs: WIRE_PROTOCOL_CURSOR_TIMEOUT_MS,
  cleanupIntervalMs: WIRE_PROTOCOL_CURSOR_CLEANUP_INTERVAL_MS,
});

/**
 * Get the global cursor store.
 * Used for testing and monitoring cursor state.
 */
export function getCursorStore(): CursorStore {
  return cursorStore;
}

// ============================================================================
// Find Command Handlers
// ============================================================================

export async function handleFind(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  if (!cmd.collection) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        2,
        'Collection name required for find',
        'InvalidNamespace'
      ),
    };
  }

  const db = ctx.client.db(cmd.database);
  const collection = db.collection(cmd.collection);

  const filter = (cmd.body.filter || {}) as Filter<MongoDocument>;
  const projection = cmd.body.projection as { [key: string]: 0 | 1 } | undefined;
  const sort = cmd.body.sort as { [key: string]: 1 | -1 } | undefined;
  const limit = cmd.body.limit as number | undefined;
  const skip = cmd.body.skip as number | undefined;
  const batchSize = (cmd.body.batchSize as number | undefined) || 101;
  const singleBatch = cmd.body.singleBatch as boolean | undefined;

  // Create cursor with options
  const cursor = collection.find(filter, {
    projection,
    sort,
    limit,
    skip,
  });

  // Set batch size
  cursor.batchSize(batchSize);

  // Get first batch
  const firstBatch = await cursor.getFirstBatch(batchSize);

  // Determine if we need to keep the cursor open
  let cursorId = 0n;
  if (!singleBatch && !cursor.isExhausted) {
    // Store cursor for getMore operations
    // FindCursor<T> is compatible with FindCursor<MongoDocument> for cursor store
    const storedCursor: FindCursor<MongoDocument> = cursor;
    cursorStore.add(storedCursor);
    cursorId = cursor.cursorId;
  }

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      cursor: {
        firstBatch,
        id: cursorId,
        ns: `${cmd.database}.${cmd.collection}`,
      },
    }),
  };
}

export async function handleCount(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  if (!cmd.collection) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        2,
        'Collection name required for count',
        'InvalidNamespace'
      ),
    };
  }

  const db = ctx.client.db(cmd.database);
  const collection = db.collection(cmd.collection);

  const filter = (cmd.body.query || {}) as Filter<MongoDocument>;
  const count = await collection.countDocuments(filter);

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      n: count,
    }),
  };
}

export async function handleDistinct(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  if (!cmd.collection) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        2,
        'Collection name required for distinct',
        'InvalidNamespace'
      ),
    };
  }

  const db = ctx.client.db(cmd.database);
  const collection = db.collection(cmd.collection);

  const key = cmd.body.key as string;
  const filter = (cmd.body.query || {}) as Filter<MongoDocument>;

  const values = await collection.distinct(key as keyof MongoDocument, filter);

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      values,
    }),
  };
}

export async function handleGetMore(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  // Extract cursor ID from command
  const cursorId = cmd.body.getMore as bigint | number;
  const collection = cmd.body.collection as string;
  const batchSize = (cmd.body.batchSize as number | undefined) || 101;

  if (!cursorId) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        2,
        'Cursor ID is required for getMore',
        'InvalidCursor'
      ),
    };
  }

  // Look up cursor in store
  const cursor = cursorStore.get(BigInt(cursorId));

  if (!cursor) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        43, // CursorNotFound
        `cursor id ${cursorId} not found`,
        'CursorNotFound'
      ),
    };
  }

  // Check if cursor has timed out
  if (cursor.isTimedOut()) {
    cursorStore.remove(BigInt(cursorId));
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        43,
        `cursor id ${cursorId} not found (timed out)`,
        'CursorNotFound'
      ),
    };
  }

  // Get next batch of documents
  const nextBatch = await cursor.getNextBatch(batchSize);

  // Determine if cursor should remain open
  let returnCursorId = BigInt(cursorId);
  if (cursor.isExhausted || cursor.isClosed) {
    // Remove exhausted cursor from store
    cursorStore.remove(BigInt(cursorId));
    returnCursorId = 0n;
  }

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      cursor: {
        nextBatch,
        id: returnCursorId,
        ns: collection ? `${cmd.database}.${collection}` : cursor.namespace,
      },
    }),
  };
}

export async function handleKillCursors(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  const cursors = cmd.body.cursors as (bigint | number)[];

  if (!cursors || !Array.isArray(cursors)) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        2,
        'cursors array is required for killCursors',
        'InvalidParameter'
      ),
    };
  }

  const cursorsKilled: bigint[] = [];
  const cursorsNotFound: bigint[] = [];
  const cursorsAlive: bigint[] = [];
  const cursorsUnknown: bigint[] = [];

  for (const cursorId of cursors) {
    const bigCursorId = BigInt(cursorId);
    const cursor = cursorStore.get(bigCursorId);

    if (cursor) {
      if (cursorStore.remove(bigCursorId)) {
        cursorsKilled.push(bigCursorId);
      } else {
        cursorsUnknown.push(bigCursorId);
      }
    } else {
      cursorsNotFound.push(bigCursorId);
    }
  }

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      cursorsKilled,
      cursorsNotFound,
      cursorsAlive,
      cursorsUnknown,
    }),
  };
}

/**
 * Find command handlers registry
 */
export const findHandlers: Record<string, CommandHandler> = {
  find: handleFind,
  count: handleCount,
  distinct: handleDistinct,
  getMore: handleGetMore,
  killCursors: handleKillCursors,
};
