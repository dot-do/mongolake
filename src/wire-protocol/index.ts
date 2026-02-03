/**
 * MongoDB Wire Protocol Module
 *
 * Provides MongoDB wire protocol parsing, serialization, and TCP server.
 */

// Message parsing
export {
  parseMessage,
  parseMessageHeader,
  parseOpMsg,
  parseOpQuery,
  extractCommand,
  StreamingMessageParser,
  OpCode,
  OpMsgFlags,
  type MessageHeader,
  type OpMsgMessage,
  type OpMsgSection,
  type OpQueryMessage,
  type ParsedMessage,
  type ExtractedCommand,
  type Document,
  type StreamingParserState,
  type StreamingParserResult,
} from './message-parser.js';

// BSON serialization and response building
export {
  // Core serialization
  serializeDocument,
  serializeDocumentPooled,
  // Wire protocol message builders
  buildOpMsgResponse,
  buildOpReply,
  buildOpCompressed,
  buildCompressedResponse,
  // Convenience response builders
  buildSuccessResponse,
  buildErrorResponse,
  buildCursorResponse,
  buildWriteResultResponse,
  // Fluent response builder API
  ResponseBuilder,
  OpMsgResponseFlags,
  // Error codes enum
  MongoErrorCode,
  getErrorCodeName,
  // Compression support
  CompressionAlgorithm,
  type CompressionContext,
  type CompressorFunction,
  // Buffer pool access
  getBufferPool,
  type BufferPoolBucketStats,
  type PooledSerializationResult,
  // Checksum utilities
  calculateCrc32c,
} from './bson-serializer.js';

// Command handlers
export {
  executeCommand,
  getCursorStore,
  type CommandContext,
  type CommandResult,
} from './command-handlers.js';

// TCP Server
export {
  createServer,
  main as startServer,
  type TcpServer,
  type TcpServerOptions,
  type TlsOptions,
  type BackpressureConfig,
  type BufferPoolConfig,
  type ShutdownConfig,
  type BufferPoolStats,
  type MessageSizeConfig,
  type MessageSizeValidationResult,
} from './tcp-server.js';

// Connection Pool
export {
  ConnectionPool,
  type ConnectionPoolConfig,
  type PooledConnection,
  type PoolMetrics,
  type ConnectionPoolEvents,
} from './connection-pool.js';

// Command Decoder (type-safe command parsing with discriminated unions)
export {
  decodeCommand,
  CommandValidationError,
  COLLECTION_COMMANDS,
  ADMIN_COMMANDS,
  CURSOR_COMMANDS,
  // Type guards
  isCollectionCommand,
  isFindCommand,
  isInsertCommand,
  isUpdateCommand,
  isDeleteCommand,
  isAggregateCommand,
  isCursorCommand,
  // Builders
  FindCommandBuilder,
  InsertCommandBuilder,
  AggregateCommandBuilder,
  findCommand,
  insertCommand,
  aggregateCommand,
  // Types
  type DecodedCommand,
  type FindCommand,
  type InsertCommand,
  type UpdateCommand,
  type DeleteCommand,
  type AggregateCommand,
  type CountCommand,
  type DistinctCommand,
  type FindAndModifyCommand,
  type GetMoreCommand,
  type KillCursorsCommand,
  type CreateIndexesCommand,
  type DropIndexesCommand,
  type ListIndexesCommand,
  type CreateCommand,
  type DropCommand,
  type PingCommand,
  type HelloCommand,
  type IsMasterCommand,
  type ListDatabasesCommand,
  type ListCollectionsCommand,
  type DropDatabaseCommand,
  type UpdateSpec,
  type DeleteSpec,
} from './command-decoder.js';

// Command Router
export {
  CommandRouter,
  createCommandRouter,
  type ParsedCommand,
  type CommandHandler,
  type CommandHandlers,
  type CommandResponse,
} from './command-router.js';

// Size Limits (request/response/document/batch validation)
export {
  // Validator class
  SizeLimitValidator,
  StreamingResponseBuilder,
  SizeLimitError,
  // Factory functions
  createSizeLimitValidator,
  createStreamingResponseBuilder,
  // Utility functions
  isValidRequestSize,
  isValidDocumentSize,
  formatBytes,
  buildSizeLimitErrorResponse,
  // Constants
  MONGODB_MAX_DOCUMENT_SIZE,
  MONGODB_MAX_MESSAGE_SIZE,
  MONGODB_MAX_BATCH_COUNT,
  DEFAULT_MAX_BATCH_BYTES,
  DEFAULT_CURSOR_BATCH_TARGET_SIZE,
  DEFAULT_SIZE_LIMITS,
  SizeLimitErrorCode,
  // Types
  type SizeLimitConfig,
  type ResolvedSizeLimitConfig,
  type SizeValidationResult,
  type BatchValidationResult,
  type StreamedResponseResult,
} from './size-limits.js';
