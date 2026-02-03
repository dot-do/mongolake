/**
 * MongoDB Wire Protocol Command Decoder
 *
 * Provides type-safe command decoding with discriminated unions,
 * validation, and builder pattern support.
 *
 * Features:
 * - Discriminated union types for commands
 * - Validation of required fields
 * - Builder pattern for constructing commands
 * - Support for all MongoDB commands
 */

import type { Document as MongoDocument, Filter, Update, BSONValue } from '../types.js';

// ============================================================================
// Type Helpers
// ============================================================================

/**
 * Safely cast a value to BSONValue for inclusion in MongoDocument.
 * This replaces `as unknown as BSONValue` double-casts by centralizing
 * the type assertion in one place with explicit documentation.
 *
 * MongoDB documents can contain complex nested types that are structurally
 * compatible with BSONValue but TypeScript cannot verify statically.
 */
function asBSON<T>(value: T): BSONValue {
  return value as BSONValue;
}

// ============================================================================
// Command Type Constants
// ============================================================================

/** Commands that require a collection name */
export const COLLECTION_COMMANDS = [
  'find',
  'insert',
  'update',
  'delete',
  'aggregate',
  'count',
  'distinct',
  'findAndModify',
  'mapReduce',
  'createIndexes',
  'dropIndexes',
  'listIndexes',
  'drop',
  'create',
  'collMod',
] as const;

/** Admin commands (no collection) */
export const ADMIN_COMMANDS = [
  'ping',
  'hello',
  'isMaster',
  'ismaster',
  'buildInfo',
  'serverStatus',
  'listDatabases',
  'listCollections',
  'getParameter',
  'setParameter',
  'currentOp',
  'killOp',
  'replSetGetStatus',
  'whatsmyuri',
  'getLog',
  'hostInfo',
  'getCmdLineOpts',
  'getFreeMonitoringStatus',
  'saslStart',
  'saslContinue',
  'endSessions',
  'killCursors',
  'dropDatabase',
] as const;

/** Cursor commands */
export const CURSOR_COMMANDS = ['getMore', 'killCursors'] as const;

export type CollectionCommandName = (typeof COLLECTION_COMMANDS)[number];
export type AdminCommandName = (typeof ADMIN_COMMANDS)[number];
export type CursorCommandName = (typeof CURSOR_COMMANDS)[number];
export type CommandName = CollectionCommandName | AdminCommandName | CursorCommandName;

// ============================================================================
// Discriminated Union Command Types
// ============================================================================

/** Base command interface */
interface BaseCommand {
  /** Command name */
  readonly name: string;
  /** Target database */
  readonly database: string;
  /** Raw command body */
  readonly body: MongoDocument;
}

/** Find command */
export interface FindCommand extends BaseCommand {
  readonly name: 'find';
  readonly collection: string;
  readonly filter?: Filter<MongoDocument>;
  readonly projection?: Record<string, 0 | 1>;
  readonly sort?: Record<string, 1 | -1>;
  readonly limit?: number;
  readonly skip?: number;
  readonly batchSize?: number;
  readonly singleBatch?: boolean;
  readonly hint?: string | Record<string, 1 | -1>;
  readonly maxTimeMS?: number;
}

/** Insert command */
export interface InsertCommand extends BaseCommand {
  readonly name: 'insert';
  readonly collection: string;
  readonly documents: MongoDocument[];
  readonly ordered?: boolean;
}

/** Update specification */
export interface UpdateSpec {
  readonly q: Filter<MongoDocument>;
  readonly u: Update<MongoDocument> | MongoDocument[];
  readonly upsert?: boolean;
  readonly multi?: boolean;
  readonly arrayFilters?: Filter<MongoDocument>[];
  readonly hint?: string | Record<string, 1 | -1>;
}

/** Update command */
export interface UpdateCommand extends BaseCommand {
  readonly name: 'update';
  readonly collection: string;
  readonly updates: UpdateSpec[];
  readonly ordered?: boolean;
}

/** Delete specification */
export interface DeleteSpec {
  readonly q: Filter<MongoDocument>;
  readonly limit: 0 | 1;
  readonly hint?: string | Record<string, 1 | -1>;
}

/** Delete command */
export interface DeleteCommand extends BaseCommand {
  readonly name: 'delete';
  readonly collection: string;
  readonly deletes: DeleteSpec[];
  readonly ordered?: boolean;
}

/** Aggregate command */
export interface AggregateCommand extends BaseCommand {
  readonly name: 'aggregate';
  readonly collection: string;
  readonly pipeline: MongoDocument[];
  readonly cursor?: { batchSize?: number };
  readonly allowDiskUse?: boolean;
  readonly maxTimeMS?: number;
  readonly hint?: string | Record<string, 1 | -1>;
}

/** Count command */
export interface CountCommand extends BaseCommand {
  readonly name: 'count';
  readonly collection: string;
  readonly query?: Filter<MongoDocument>;
  readonly limit?: number;
  readonly skip?: number;
  readonly hint?: string | Record<string, 1 | -1>;
  readonly maxTimeMS?: number;
}

/** Distinct command */
export interface DistinctCommand extends BaseCommand {
  readonly name: 'distinct';
  readonly collection: string;
  readonly key: string;
  readonly query?: Filter<MongoDocument>;
  readonly maxTimeMS?: number;
}

/** FindAndModify command */
export interface FindAndModifyCommand extends BaseCommand {
  readonly name: 'findAndModify';
  readonly collection: string;
  readonly query?: Filter<MongoDocument>;
  readonly sort?: Record<string, 1 | -1>;
  readonly remove?: boolean;
  readonly update?: Update<MongoDocument> | MongoDocument[];
  readonly new?: boolean;
  readonly fields?: Record<string, 0 | 1>;
  readonly upsert?: boolean;
  readonly arrayFilters?: Filter<MongoDocument>[];
  readonly maxTimeMS?: number;
}

/** GetMore command */
export interface GetMoreCommand extends BaseCommand {
  readonly name: 'getMore';
  readonly cursorId: bigint | number;
  readonly collection: string;
  readonly batchSize?: number;
  readonly maxTimeMS?: number;
}

/** KillCursors command */
export interface KillCursorsCommand extends BaseCommand {
  readonly name: 'killCursors';
  readonly collection: string;
  readonly cursors: (bigint | number)[];
}

/** CreateIndexes command */
export interface CreateIndexesCommand extends BaseCommand {
  readonly name: 'createIndexes';
  readonly collection: string;
  readonly indexes: Array<{
    key: Record<string, 1 | -1 | 'text' | '2dsphere' | 'hashed'>;
    name?: string;
    unique?: boolean;
    sparse?: boolean;
    expireAfterSeconds?: number;
    partialFilterExpression?: Filter<MongoDocument>;
  }>;
}

/** DropIndexes command */
export interface DropIndexesCommand extends BaseCommand {
  readonly name: 'dropIndexes';
  readonly collection: string;
  readonly index: string | Record<string, 1 | -1> | '*';
}

/** ListIndexes command */
export interface ListIndexesCommand extends BaseCommand {
  readonly name: 'listIndexes';
  readonly collection: string;
  readonly cursor?: { batchSize?: number };
}

/** Create command */
export interface CreateCommand extends BaseCommand {
  readonly name: 'create';
  readonly collection: string;
  readonly capped?: boolean;
  readonly size?: number;
  readonly max?: number;
  readonly validator?: MongoDocument;
  readonly validationLevel?: 'off' | 'strict' | 'moderate';
  readonly validationAction?: 'error' | 'warn';
}

/** Drop command */
export interface DropCommand extends BaseCommand {
  readonly name: 'drop';
  readonly collection: string;
}

/** Ping command */
export interface PingCommand extends BaseCommand {
  readonly name: 'ping';
}

/** Hello command */
export interface HelloCommand extends BaseCommand {
  readonly name: 'hello';
}

/** IsMaster command */
export interface IsMasterCommand extends BaseCommand {
  readonly name: 'isMaster' | 'ismaster';
}

/** ListDatabases command */
export interface ListDatabasesCommand extends BaseCommand {
  readonly name: 'listDatabases';
  readonly nameOnly?: boolean;
  readonly filter?: Filter<MongoDocument>;
}

/** ListCollections command */
export interface ListCollectionsCommand extends BaseCommand {
  readonly name: 'listCollections';
  readonly filter?: Filter<MongoDocument>;
  readonly nameOnly?: boolean;
  readonly cursor?: { batchSize?: number };
}

/** DropDatabase command */
export interface DropDatabaseCommand extends BaseCommand {
  readonly name: 'dropDatabase';
}

/** BuildInfo command */
export interface BuildInfoCommand extends BaseCommand {
  readonly name: 'buildInfo';
}

/** ServerStatus command */
export interface ServerStatusCommand extends BaseCommand {
  readonly name: 'serverStatus';
}

/** Generic admin command */
export interface GenericAdminCommand extends BaseCommand {
  readonly name: AdminCommandName;
}

/** Discriminated union of all command types */
export type DecodedCommand =
  | FindCommand
  | InsertCommand
  | UpdateCommand
  | DeleteCommand
  | AggregateCommand
  | CountCommand
  | DistinctCommand
  | FindAndModifyCommand
  | GetMoreCommand
  | KillCursorsCommand
  | CreateIndexesCommand
  | DropIndexesCommand
  | ListIndexesCommand
  | CreateCommand
  | DropCommand
  | PingCommand
  | HelloCommand
  | IsMasterCommand
  | ListDatabasesCommand
  | ListCollectionsCommand
  | DropDatabaseCommand
  | BuildInfoCommand
  | ServerStatusCommand
  | GenericAdminCommand;

// ============================================================================
// Validation Errors
// ============================================================================

/** Command validation error */
export class CommandValidationError extends Error {
  readonly code: number;
  readonly codeName: string;

  constructor(message: string, code: number = 2, codeName: string = 'BadValue') {
    super(message);
    this.name = 'CommandValidationError';
    this.code = code;
    this.codeName = codeName;
  }
}

// ============================================================================
// Validation Functions
// ============================================================================

/**
 * Validate that a required string field is present and non-empty
 */
function validateRequiredString(
  value: unknown,
  fieldName: string
): asserts value is string {
  if (typeof value !== 'string') {
    throw new CommandValidationError(
      `${fieldName} must be a string`,
      14,
      'TypeMismatch'
    );
  }
  if (value === '') {
    throw new CommandValidationError(
      `${fieldName} cannot be empty`,
      2,
      'BadValue'
    );
  }
}

/**
 * Validate that a required array field is present
 */
function validateRequiredArray(
  value: unknown,
  fieldName: string
): asserts value is unknown[] {
  if (!Array.isArray(value)) {
    throw new CommandValidationError(
      `${fieldName} must be an array`,
      14,
      'TypeMismatch'
    );
  }
}

/**
 * Validate command has $db field
 */
function validateDatabase(body: MongoDocument): string {
  const db = body['$db'];
  if (typeof db !== 'string' || db === '') {
    throw new CommandValidationError(
      '$db field is required',
      2,
      'BadValue'
    );
  }
  return db;
}

/**
 * Get command name from body (first non-special field)
 */
function getCommandName(body: MongoDocument): string {
  for (const key of Object.keys(body)) {
    if (!key.startsWith('$')) {
      return key;
    }
  }
  throw new CommandValidationError(
    'Could not find command name in body',
    59,
    'CommandNotFound'
  );
}

/**
 * Get collection name from body for collection commands
 */
function getCollectionName(body: MongoDocument, commandName: string): string {
  const collection = body[commandName];
  validateRequiredString(collection, 'Collection name');
  return collection;
}

// ============================================================================
// Command Decoders
// ============================================================================

function decodeFindCommand(
  body: MongoDocument,
  database: string,
  _documents?: MongoDocument[]
): FindCommand {
  const collection = getCollectionName(body, 'find');
  return {
    name: 'find',
    database,
    collection,
    filter: body.filter as Filter<MongoDocument> | undefined,
    projection: body.projection as Record<string, 0 | 1> | undefined,
    sort: body.sort as Record<string, 1 | -1> | undefined,
    limit: body.limit as number | undefined,
    skip: body.skip as number | undefined,
    batchSize: body.batchSize as number | undefined,
    singleBatch: body.singleBatch as boolean | undefined,
    hint: body.hint as string | Record<string, 1 | -1> | undefined,
    maxTimeMS: body.maxTimeMS as number | undefined,
    body,
  };
}

function decodeInsertCommand(
  body: MongoDocument,
  database: string,
  documents?: MongoDocument[]
): InsertCommand {
  const collection = getCollectionName(body, 'insert');
  // Get documents from sequence sections or body. Empty array is allowed
  // at decode time - validation of non-empty can happen at execution.
  const docs = documents ?? (body.documents as MongoDocument[] | undefined) ?? [];

  return {
    name: 'insert',
    database,
    collection,
    documents: docs,
    ordered: body.ordered as boolean | undefined,
    body,
  };
}

function decodeUpdateCommand(
  body: MongoDocument,
  database: string,
  documents?: MongoDocument[]
): UpdateCommand {
  const collection = getCollectionName(body, 'update');
  // Get updates from sequence sections or body. Empty array is allowed
  // at decode time - validation of non-empty can happen at execution.
  const updates = documents ?? (body.updates as UpdateSpec[] | undefined) ?? [];

  return {
    name: 'update',
    database,
    collection,
    updates: updates as UpdateSpec[],
    ordered: body.ordered as boolean | undefined,
    body,
  };
}

function decodeDeleteCommand(
  body: MongoDocument,
  database: string,
  documents?: MongoDocument[]
): DeleteCommand {
  const collection = getCollectionName(body, 'delete');
  // Get deletes from sequence sections or body. Empty array is allowed
  // at decode time - validation of non-empty can happen at execution.
  const deletes = documents ?? (body.deletes as DeleteSpec[] | undefined) ?? [];

  return {
    name: 'delete',
    database,
    collection,
    deletes: deletes as DeleteSpec[],
    ordered: body.ordered as boolean | undefined,
    body,
  };
}

function decodeAggregateCommand(
  body: MongoDocument,
  database: string
): AggregateCommand {
  const collection = getCollectionName(body, 'aggregate');
  const pipeline = body.pipeline;
  validateRequiredArray(pipeline, 'pipeline');

  return {
    name: 'aggregate',
    database,
    collection,
    pipeline: pipeline as MongoDocument[],
    cursor: body.cursor as { batchSize?: number } | undefined,
    allowDiskUse: body.allowDiskUse as boolean | undefined,
    maxTimeMS: body.maxTimeMS as number | undefined,
    hint: body.hint as string | Record<string, 1 | -1> | undefined,
    body,
  };
}

function decodeCountCommand(
  body: MongoDocument,
  database: string
): CountCommand {
  const collection = getCollectionName(body, 'count');

  return {
    name: 'count',
    database,
    collection,
    query: body.query as Filter<MongoDocument> | undefined,
    limit: body.limit as number | undefined,
    skip: body.skip as number | undefined,
    hint: body.hint as string | Record<string, 1 | -1> | undefined,
    maxTimeMS: body.maxTimeMS as number | undefined,
    body,
  };
}

function decodeDistinctCommand(
  body: MongoDocument,
  database: string
): DistinctCommand {
  const collection = getCollectionName(body, 'distinct');
  const key = body.key;
  validateRequiredString(key, 'key');

  return {
    name: 'distinct',
    database,
    collection,
    key,
    query: body.query as Filter<MongoDocument> | undefined,
    maxTimeMS: body.maxTimeMS as number | undefined,
    body,
  };
}

function decodeFindAndModifyCommand(
  body: MongoDocument,
  database: string
): FindAndModifyCommand {
  const collection = getCollectionName(body, 'findAndModify');

  // Either update or remove must be specified
  if (body.update === undefined && !body.remove) {
    throw new CommandValidationError(
      'Either update or remove must be specified',
      9,
      'FailedToParse'
    );
  }

  return {
    name: 'findAndModify',
    database,
    collection,
    query: body.query as Filter<MongoDocument> | undefined,
    sort: body.sort as Record<string, 1 | -1> | undefined,
    remove: body.remove as boolean | undefined,
    update: body.update as Update<MongoDocument> | MongoDocument[] | undefined,
    new: body.new as boolean | undefined,
    fields: body.fields as Record<string, 0 | 1> | undefined,
    upsert: body.upsert as boolean | undefined,
    arrayFilters: body.arrayFilters as Filter<MongoDocument>[] | undefined,
    maxTimeMS: body.maxTimeMS as number | undefined,
    body,
  };
}

function decodeGetMoreCommand(
  body: MongoDocument,
  database: string
): GetMoreCommand {
  const cursorId = body.getMore;
  if (typeof cursorId !== 'bigint' && typeof cursorId !== 'number') {
    throw new CommandValidationError(
      'getMore cursor id must be a number or bigint',
      14,
      'TypeMismatch'
    );
  }

  const collection = body.collection;
  if (typeof collection !== 'string') {
    throw new CommandValidationError(
      'collection field is required for getMore',
      2,
      'BadValue'
    );
  }

  return {
    name: 'getMore',
    database,
    cursorId,
    collection,
    batchSize: body.batchSize as number | undefined,
    maxTimeMS: body.maxTimeMS as number | undefined,
    body,
  };
}

function decodeKillCursorsCommand(
  body: MongoDocument,
  database: string
): KillCursorsCommand {
  const collection = getCollectionName(body, 'killCursors');
  const cursors = body.cursors;
  validateRequiredArray(cursors, 'cursors');

  return {
    name: 'killCursors',
    database,
    collection,
    cursors: cursors as (bigint | number)[],
    body,
  };
}

function decodeCreateIndexesCommand(
  body: MongoDocument,
  database: string
): CreateIndexesCommand {
  const collection = getCollectionName(body, 'createIndexes');
  const indexes = body.indexes;
  validateRequiredArray(indexes, 'indexes');

  return {
    name: 'createIndexes',
    database,
    collection,
    indexes: indexes as CreateIndexesCommand['indexes'],
    body,
  };
}

function decodeDropIndexesCommand(
  body: MongoDocument,
  database: string
): DropIndexesCommand {
  const collection = getCollectionName(body, 'dropIndexes');
  const index = body.index;

  if (index === undefined) {
    throw new CommandValidationError(
      'index field is required for dropIndexes',
      2,
      'BadValue'
    );
  }

  return {
    name: 'dropIndexes',
    database,
    collection,
    index: index as string | Record<string, 1 | -1> | '*',
    body,
  };
}

function decodeListIndexesCommand(
  body: MongoDocument,
  database: string
): ListIndexesCommand {
  const collection = getCollectionName(body, 'listIndexes');

  return {
    name: 'listIndexes',
    database,
    collection,
    cursor: body.cursor as { batchSize?: number } | undefined,
    body,
  };
}

function decodeCreateCommand(
  body: MongoDocument,
  database: string
): CreateCommand {
  const collection = getCollectionName(body, 'create');

  return {
    name: 'create',
    database,
    collection,
    capped: body.capped as boolean | undefined,
    size: body.size as number | undefined,
    max: body.max as number | undefined,
    validator: body.validator as MongoDocument | undefined,
    validationLevel: body.validationLevel as 'off' | 'strict' | 'moderate' | undefined,
    validationAction: body.validationAction as 'error' | 'warn' | undefined,
    body,
  };
}

function decodeDropCommand(
  body: MongoDocument,
  database: string
): DropCommand {
  const collection = getCollectionName(body, 'drop');

  return {
    name: 'drop',
    database,
    collection,
    body,
  };
}

function decodeListCollectionsCommand(
  body: MongoDocument,
  database: string
): ListCollectionsCommand {
  return {
    name: 'listCollections',
    database,
    filter: body.filter as Filter<MongoDocument> | undefined,
    nameOnly: body.nameOnly as boolean | undefined,
    cursor: body.cursor as { batchSize?: number } | undefined,
    body,
  };
}

function decodeListDatabasesCommand(
  body: MongoDocument,
  database: string
): ListDatabasesCommand {
  return {
    name: 'listDatabases',
    database,
    nameOnly: body.nameOnly as boolean | undefined,
    filter: body.filter as Filter<MongoDocument> | undefined,
    body,
  };
}

// ============================================================================
// Main Decoder Function
// ============================================================================

/**
 * Decode a command from raw body data
 *
 * @param body - The command body document
 * @param documents - Optional documents from document sequence sections
 * @returns Decoded and validated command
 * @throws CommandValidationError if validation fails
 */
export function decodeCommand(
  body: MongoDocument,
  documents?: MongoDocument[]
): DecodedCommand {
  const database = validateDatabase(body);
  const commandName = getCommandName(body);

  switch (commandName) {
    case 'find':
      return decodeFindCommand(body, database, documents);
    case 'insert':
      return decodeInsertCommand(body, database, documents);
    case 'update':
      return decodeUpdateCommand(body, database, documents);
    case 'delete':
      return decodeDeleteCommand(body, database, documents);
    case 'aggregate':
      return decodeAggregateCommand(body, database);
    case 'count':
      return decodeCountCommand(body, database);
    case 'distinct':
      return decodeDistinctCommand(body, database);
    case 'findAndModify':
      return decodeFindAndModifyCommand(body, database);
    case 'getMore':
      return decodeGetMoreCommand(body, database);
    case 'killCursors':
      return decodeKillCursorsCommand(body, database);
    case 'createIndexes':
      return decodeCreateIndexesCommand(body, database);
    case 'dropIndexes':
      return decodeDropIndexesCommand(body, database);
    case 'listIndexes':
      return decodeListIndexesCommand(body, database);
    case 'create':
      return decodeCreateCommand(body, database);
    case 'drop':
      return decodeDropCommand(body, database);
    case 'listCollections':
      return decodeListCollectionsCommand(body, database);
    case 'listDatabases':
      return decodeListDatabasesCommand(body, database);
    case 'dropDatabase':
      return { name: 'dropDatabase', database, body } as DropDatabaseCommand;
    case 'ping':
      return { name: 'ping', database, body } as PingCommand;
    case 'hello':
      return { name: 'hello', database, body } as HelloCommand;
    case 'isMaster':
    case 'ismaster':
      return { name: commandName as 'isMaster' | 'ismaster', database, body } as IsMasterCommand;
    case 'buildInfo':
      return { name: 'buildInfo', database, body } as BuildInfoCommand;
    case 'serverStatus':
      return { name: 'serverStatus', database, body } as ServerStatusCommand;
    default:
      // Generic admin command
      return { name: commandName, database, body } as GenericAdminCommand;
  }
}

// ============================================================================
// Type Guards
// ============================================================================

/** Check if command is a collection command */
export function isCollectionCommand(
  cmd: DecodedCommand
): cmd is DecodedCommand & { collection: string } {
  return 'collection' in cmd && typeof cmd.collection === 'string';
}

/** Check if command is a find command */
export function isFindCommand(cmd: DecodedCommand): cmd is FindCommand {
  return cmd.name === 'find';
}

/** Check if command is an insert command */
export function isInsertCommand(cmd: DecodedCommand): cmd is InsertCommand {
  return cmd.name === 'insert';
}

/** Check if command is an update command */
export function isUpdateCommand(cmd: DecodedCommand): cmd is UpdateCommand {
  return cmd.name === 'update';
}

/** Check if command is a delete command */
export function isDeleteCommand(cmd: DecodedCommand): cmd is DeleteCommand {
  return cmd.name === 'delete';
}

/** Check if command is an aggregate command */
export function isAggregateCommand(cmd: DecodedCommand): cmd is AggregateCommand {
  return cmd.name === 'aggregate';
}

/** Check if command is a cursor command */
export function isCursorCommand(
  cmd: DecodedCommand
): cmd is GetMoreCommand | KillCursorsCommand {
  return cmd.name === 'getMore' || cmd.name === 'killCursors';
}

// ============================================================================
// Command Builder Pattern
// ============================================================================

/**
 * Builder for constructing find commands
 */
export class FindCommandBuilder {
  private _collection: string = '';
  private _database: string = '';
  private _filter?: Filter<MongoDocument>;
  private _projection?: Record<string, 0 | 1>;
  private _sort?: Record<string, 1 | -1>;
  private _limit?: number;
  private _skip?: number;
  private _batchSize?: number;
  private _singleBatch?: boolean;
  private _hint?: string | Record<string, 1 | -1>;
  private _maxTimeMS?: number;

  collection(name: string): this {
    this._collection = name;
    return this;
  }

  database(name: string): this {
    this._database = name;
    return this;
  }

  filter(filter: Filter<MongoDocument>): this {
    this._filter = filter;
    return this;
  }

  projection(projection: Record<string, 0 | 1>): this {
    this._projection = projection;
    return this;
  }

  sort(sort: Record<string, 1 | -1>): this {
    this._sort = sort;
    return this;
  }

  limit(limit: number): this {
    this._limit = limit;
    return this;
  }

  skip(skip: number): this {
    this._skip = skip;
    return this;
  }

  batchSize(batchSize: number): this {
    this._batchSize = batchSize;
    return this;
  }

  singleBatch(singleBatch: boolean): this {
    this._singleBatch = singleBatch;
    return this;
  }

  hint(hint: string | Record<string, 1 | -1>): this {
    this._hint = hint;
    return this;
  }

  maxTimeMS(maxTimeMS: number): this {
    this._maxTimeMS = maxTimeMS;
    return this;
  }

  build(): FindCommand {
    if (!this._collection) {
      throw new CommandValidationError('Collection name is required');
    }
    if (!this._database) {
      throw new CommandValidationError('Database name is required');
    }

    const body: MongoDocument = {
      find: this._collection,
      $db: this._database,
    };
    if (this._filter) body.filter = asBSON(this._filter);
    if (this._projection) body.projection = asBSON(this._projection);
    if (this._sort) body.sort = asBSON(this._sort);
    if (this._limit !== undefined) body.limit = this._limit;
    if (this._skip !== undefined) body.skip = this._skip;
    if (this._batchSize !== undefined) body.batchSize = this._batchSize;
    if (this._singleBatch !== undefined) body.singleBatch = this._singleBatch;
    if (this._hint) body.hint = asBSON(this._hint);
    if (this._maxTimeMS !== undefined) body.maxTimeMS = this._maxTimeMS;

    return {
      name: 'find',
      database: this._database,
      collection: this._collection,
      filter: this._filter,
      projection: this._projection,
      sort: this._sort,
      limit: this._limit,
      skip: this._skip,
      batchSize: this._batchSize,
      singleBatch: this._singleBatch,
      hint: this._hint,
      maxTimeMS: this._maxTimeMS,
      body,
    };
  }
}

/**
 * Builder for constructing insert commands
 */
export class InsertCommandBuilder {
  private _collection: string = '';
  private _database: string = '';
  private _documents: MongoDocument[] = [];
  private _ordered: boolean = true;

  collection(name: string): this {
    this._collection = name;
    return this;
  }

  database(name: string): this {
    this._database = name;
    return this;
  }

  documents(docs: MongoDocument[]): this {
    this._documents = docs;
    return this;
  }

  document(doc: MongoDocument): this {
    this._documents.push(doc);
    return this;
  }

  ordered(ordered: boolean): this {
    this._ordered = ordered;
    return this;
  }

  build(): InsertCommand {
    if (!this._collection) {
      throw new CommandValidationError('Collection name is required');
    }
    if (!this._database) {
      throw new CommandValidationError('Database name is required');
    }
    if (this._documents.length === 0) {
      throw new CommandValidationError('At least one document is required');
    }

    const body: MongoDocument = {
      insert: this._collection,
      $db: this._database,
      documents: asBSON(this._documents),
      ordered: this._ordered,
    };

    return {
      name: 'insert',
      database: this._database,
      collection: this._collection,
      documents: this._documents,
      ordered: this._ordered,
      body,
    };
  }
}

/**
 * Builder for constructing aggregate commands
 */
export class AggregateCommandBuilder {
  private _collection: string = '';
  private _database: string = '';
  private _pipeline: MongoDocument[] = [];
  private _cursor?: { batchSize?: number };
  private _allowDiskUse?: boolean;
  private _maxTimeMS?: number;
  private _hint?: string | Record<string, 1 | -1>;

  collection(name: string): this {
    this._collection = name;
    return this;
  }

  database(name: string): this {
    this._database = name;
    return this;
  }

  pipeline(pipeline: MongoDocument[]): this {
    this._pipeline = pipeline;
    return this;
  }

  stage(stage: MongoDocument): this {
    this._pipeline.push(stage);
    return this;
  }

  match(filter: Filter<MongoDocument>): this {
    this._pipeline.push({ $match: asBSON(filter) });
    return this;
  }

  project(projection: Record<string, unknown>): this {
    this._pipeline.push({ $project: asBSON(projection) });
    return this;
  }

  group(groupSpec: MongoDocument): this {
    this._pipeline.push({ $group: asBSON(groupSpec) });
    return this;
  }

  sort(sort: Record<string, 1 | -1>): this {
    this._pipeline.push({ $sort: asBSON(sort) });
    return this;
  }

  limit(limit: number): this {
    this._pipeline.push({ $limit: limit });
    return this;
  }

  skip(skip: number): this {
    this._pipeline.push({ $skip: skip });
    return this;
  }

  cursor(cursor: { batchSize?: number }): this {
    this._cursor = cursor;
    return this;
  }

  allowDiskUse(allowDiskUse: boolean): this {
    this._allowDiskUse = allowDiskUse;
    return this;
  }

  maxTimeMS(maxTimeMS: number): this {
    this._maxTimeMS = maxTimeMS;
    return this;
  }

  hint(hint: string | Record<string, 1 | -1>): this {
    this._hint = hint;
    return this;
  }

  build(): AggregateCommand {
    if (!this._collection) {
      throw new CommandValidationError('Collection name is required');
    }
    if (!this._database) {
      throw new CommandValidationError('Database name is required');
    }

    const body: MongoDocument = {
      aggregate: this._collection,
      $db: this._database,
      pipeline: asBSON(this._pipeline),
    };
    if (this._cursor) body.cursor = asBSON(this._cursor);
    if (this._allowDiskUse !== undefined) body.allowDiskUse = this._allowDiskUse;
    if (this._maxTimeMS !== undefined) body.maxTimeMS = this._maxTimeMS;
    if (this._hint) body.hint = asBSON(this._hint);

    return {
      name: 'aggregate',
      database: this._database,
      collection: this._collection,
      pipeline: this._pipeline,
      cursor: this._cursor,
      allowDiskUse: this._allowDiskUse,
      maxTimeMS: this._maxTimeMS,
      hint: this._hint,
      body,
    };
  }
}

// ============================================================================
// Factory Functions
// ============================================================================

/** Create a new find command builder */
export function findCommand(): FindCommandBuilder {
  return new FindCommandBuilder();
}

/** Create a new insert command builder */
export function insertCommand(): InsertCommandBuilder {
  return new InsertCommandBuilder();
}

/** Create a new aggregate command builder */
export function aggregateCommand(): AggregateCommandBuilder {
  return new AggregateCommandBuilder();
}
