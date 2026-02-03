/**
 * Aggregate command handler for MongoDB wire protocol
 * Handles: aggregate
 */

import type { ExtractedCommand } from '../message-parser.js';
import {
  buildSuccessResponse,
  buildErrorResponse,
} from '../bson-serializer.js';
import type { AggregationStage } from '../../types.js';
import type { CommandContext, CommandResult, CommandHandler } from './types.js';

// ============================================================================
// Aggregate Command Handler
// ============================================================================

export async function handleAggregate(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  if (!cmd.collection) {
    return {
      response: buildErrorResponse(
        ctx.requestId + 1,
        ctx.requestId,
        2,
        'Collection name required for aggregate',
        'InvalidNamespace'
      ),
    };
  }

  const db = ctx.client.db(cmd.database);
  const collection = db.collection(cmd.collection);

  // Pipeline comes from wire protocol as array of stage objects
  const pipeline = (cmd.body.pipeline || []) as AggregationStage[];
  const docs = await collection.aggregate(pipeline).toArray();

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      cursor: {
        firstBatch: docs,
        id: 0n,
        ns: `${cmd.database}.${cmd.collection}`,
      },
    }),
  };
}

/**
 * Aggregate command handlers registry
 */
export const aggregateHandlers: Record<string, CommandHandler> = {
  aggregate: handleAggregate,
};
