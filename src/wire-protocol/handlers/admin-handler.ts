/**
 * Admin command handlers for MongoDB wire protocol
 * Handles: ping, hello, isMaster, buildInfo, serverStatus, listDatabases,
 * listCollections, whatsmyuri, getLog, hostInfo, getCmdLineOpts, getParameter,
 * getFreeMonitoringStatus, saslStart, saslContinue, endSessions
 */

import type { ExtractedCommand } from '../message-parser.js';
import {
  buildSuccessResponse,
} from '../bson-serializer.js';
import {
  WIRE_PROTOCOL_SERVER_VERSION,
  WIRE_PROTOCOL_VERSION_MIN,
  WIRE_PROTOCOL_VERSION_MAX,
} from '../../constants.js';
import type { CommandContext, CommandResult, CommandHandler, Document } from './types.js';

// ============================================================================
// Server Info Helper
// ============================================================================

function getServerInfo() {
  return {
    ismaster: true,
    maxBsonObjectSize: 16777216,
    maxMessageSizeBytes: 48000000,
    maxWriteBatchSize: 100000,
    localTime: new Date(),
    logicalSessionTimeoutMinutes: 30,
    connectionId: 1,
    minWireVersion: WIRE_PROTOCOL_VERSION_MIN,
    maxWireVersion: WIRE_PROTOCOL_VERSION_MAX,
    readOnly: false,
  };
}

// ============================================================================
// Admin Command Handlers
// ============================================================================

export async function handlePing(
  _cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId),
  };
}

export async function handleHello(
  _cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      ...getServerInfo(),
      helloOk: true,
    }),
  };
}

export async function handleIsMaster(
  _cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      ...getServerInfo(),
    }),
  };
}

export async function handleBuildInfo(
  _cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      version: WIRE_PROTOCOL_SERVER_VERSION,
      gitVersion: 'mongolake',
      modules: [],
      allocator: 'system',
      javascriptEngine: 'none',
      sysInfo: 'deprecated',
      versionArray: [7, 0, 0, 0],
      bits: 64,
      debug: false,
      maxBsonObjectSize: 16777216,
    }),
  };
}

export async function handleServerStatus(
  _cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      host: 'localhost',
      version: WIRE_PROTOCOL_SERVER_VERSION,
      process: 'mongolake',
      pid: process.pid || 1,
      uptime: process.uptime ? process.uptime() : 0,
      uptimeMillis: process.uptime ? Math.floor(process.uptime() * 1000) : 0,
      uptimeEstimate: process.uptime ? Math.floor(process.uptime()) : 0,
      localTime: new Date(),
      connections: {
        current: 1,
        available: 100,
        totalCreated: 1,
      },
      mem: {
        bits: 64,
        resident: 0,
        virtual: 0,
        supported: false,
      },
    }),
  };
}

export async function handleListDatabases(
  _cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  const databases = await ctx.client.listDatabases();

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      databases: databases.map((name) => ({
        name,
        sizeOnDisk: 0,
        empty: false,
      })),
      totalSize: 0,
      totalSizeMb: 0,
    }),
  };
}

export async function handleListCollections(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  const db = ctx.client.db(cmd.database);
  const collections = await db.listCollections();

  const docs = collections.map((name) => ({
    name,
    type: 'collection',
    options: {},
    info: {
      readOnly: false,
    },
    idIndex: {
      v: 2,
      key: { _id: 1 },
      name: '_id_',
    },
  }));

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      cursor: {
        id: 0n,
        ns: `${cmd.database}.$cmd.listCollections`,
        firstBatch: docs,
      },
    }),
  };
}

export async function handleWhatsMyUri(
  _cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      you: '127.0.0.1:0',
    }),
  };
}

export async function handleGetLog(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  const logType = cmd.body.getLog as string;

  if (logType === 'startupWarnings') {
    return {
      response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
        totalLinesWritten: 0,
        log: [],
      }),
    };
  }

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      totalLinesWritten: 0,
      log: [],
    }),
  };
}

export async function handleHostInfo(
  _cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      system: {
        currentTime: new Date(),
        hostname: 'localhost',
        cpuAddrSize: 64,
        numCores: 4,
        cpuArch: process.arch || 'x64',
      },
      os: {
        type: process.platform || 'Linux',
        name: 'MongoLake',
        version: '1.0',
      },
      extra: {},
    }),
  };
}

export async function handleGetCmdLineOpts(
  _cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      argv: ['mongolake'],
      parsed: {},
    }),
  };
}

export async function handleGetParameter(
  cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  const result: Document = {};

  if (cmd.body.featureCompatibilityVersion) {
    result.featureCompatibilityVersion = { version: '7.0' };
  }

  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, result),
  };
}

export async function handleGetFreeMonitoringStatus(
  _cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      state: 'disabled',
    }),
  };
}

export async function handleSaslStart(
  _cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  // Simplified SASL handling - accept any auth for now
  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      conversationId: 1,
      done: true,
      payload: new Uint8Array(0),
    }),
  };
}

export async function handleSaslContinue(
  _cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId, {
      conversationId: 1,
      done: true,
      payload: new Uint8Array(0),
    }),
  };
}

export async function handleEndSessions(
  _cmd: ExtractedCommand,
  ctx: CommandContext
): Promise<CommandResult> {
  return {
    response: buildSuccessResponse(ctx.requestId + 1, ctx.requestId),
  };
}

/**
 * Admin command handlers registry
 */
export const adminHandlers: Record<string, CommandHandler> = {
  ping: handlePing,
  hello: handleHello,
  isMaster: handleIsMaster,
  ismaster: handleIsMaster,
  buildInfo: handleBuildInfo,
  serverStatus: handleServerStatus,
  listDatabases: handleListDatabases,
  listCollections: handleListCollections,
  whatsmyuri: handleWhatsMyUri,
  getLog: handleGetLog,
  hostInfo: handleHostInfo,
  getCmdLineOpts: handleGetCmdLineOpts,
  getParameter: handleGetParameter,
  getFreeMonitoringStatus: handleGetFreeMonitoringStatus,
  saslStart: handleSaslStart,
  saslContinue: handleSaslContinue,
  endSessions: handleEndSessions,
};
