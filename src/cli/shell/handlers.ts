/**
 * Shell Command Handlers
 *
 * Executes MongoDB shell commands against a MongoLake instance.
 * Handlers are organized by category:
 * - Database operations (show dbs, use db, stats)
 * - Collection operations (CRUD, indexes, stats)
 * - Cursor operations (find with chaining)
 *
 * @module cli/shell/handlers
 */

import type { MongoLake, Document } from '../../client/index.js';
import type { Filter, Update, AggregationStage } from '../../types.js';
import { getExtendedColors } from '../utils.js';
import {
  formatOutput,
  highlightOutput,
  printSuccess,
  printError,
  printInfo,
} from './output.js';
import {
  parseMethodChain,
  parseArgs,
  jsToJson,
  type ParsedMethod,
} from './parser.js';

// ============================================================================
// Extended Collection Interface
// ============================================================================

/**
 * Extended collection interface for shell operations.
 * These methods may exist at runtime depending on collection implementation.
 */
interface ExtendedCollection {
  replaceOne(filter: Filter<Document>, replacement: Document): Promise<{
    acknowledged: boolean;
    matchedCount: number;
    modifiedCount: number;
  }>;
  distinct(field: string): Promise<unknown[]>;
  createIndex(spec: Record<string, number>): Promise<string>;
  listIndexes(): Promise<Array<{ name: string; key: unknown }>>;
  dropIndex(name: string): Promise<{ ok: number }>;
  stats(): Promise<unknown>;
  bulkWrite(operations: unknown[]): Promise<unknown>;
}

/**
 * Type guard to check if collection has extended methods
 */
function hasExtendedMethod<K extends keyof ExtendedCollection>(
  coll: unknown,
  method: K
): coll is { [P in K]: ExtendedCollection[P] } {
  return typeof (coll as Record<string, unknown>)[method] === 'function';
}

/**
 * Extended cursor interface for explain support
 */
interface ExtendedCursor {
  explain(): Promise<unknown>;
}

/**
 * Type guard for cursor with explain
 */
function hasExplain(cursor: unknown): cursor is ExtendedCursor {
  return typeof (cursor as Record<string, unknown>).explain === 'function';
}

// ============================================================================
// Types
// ============================================================================

/**
 * Shell state that handlers can read and modify
 */
export interface ShellState {
  lake: MongoLake;
  currentDb: string;
  history: string[];
  verbose: boolean;
  quiet: boolean;
  lastCursor: AsyncCursor | null;
  multiLineBuffer: string;
  isMultiLine: boolean;
  shellBatchSize: number;
  collections: string[];
}

/**
 * Cursor interface for iteration support
 */
export interface AsyncCursor {
  toArray: () => Promise<Document[]>;
  hasNext?: () => Promise<boolean>;
  next?: () => Promise<Document | null>;
}

// ============================================================================
// Help Command
// ============================================================================

/**
 * Display shell help information.
 */
export function showHelp(): void {
  const colors = getExtendedColors();
  console.log(`
${colors.bright}MongoLake Shell Commands${colors.reset}

${colors.cyan}Database Commands:${colors.reset}
  show dbs              List all databases
  use <database>        Switch to a database
  show collections      List collections in current database
  db.stats()            Show database statistics
  db.createCollection() Create a new collection
  db.dropDatabase()     Drop the current database

${colors.cyan}Collection Commands:${colors.reset}
  db.<collection>.find()                    Find all documents
  db.<collection>.find(<filter>)            Find documents matching filter
  db.<collection>.findOne(<filter>)         Find one document
  db.<collection>.insertOne(<document>)     Insert a document
  db.<collection>.insertMany([<docs>])      Insert multiple documents
  db.<collection>.updateOne(<filter>, <update>)   Update one document
  db.<collection>.updateMany(<filter>, <update>)  Update multiple documents
  db.<collection>.replaceOne(<filter>, <doc>)     Replace one document
  db.<collection>.deleteOne(<filter>)       Delete one document
  db.<collection>.deleteMany(<filter>)      Delete multiple documents
  db.<collection>.countDocuments(<filter>)  Count documents
  db.<collection>.distinct(<field>)         Get distinct values
  db.<collection>.aggregate([<pipeline>])   Run aggregation pipeline
  db.<collection>.createIndex(<keys>)       Create an index
  db.<collection>.getIndexes()              List indexes
  db.<collection>.dropIndex(<name>)         Drop an index
  db.<collection>.stats()                   Show collection statistics
  db.<collection>.bulkWrite([<ops>])        Bulk write operations

${colors.cyan}Cursor Methods:${colors.reset}
  .limit(n)             Limit results
  .skip(n)              Skip results
  .sort({field: 1})     Sort results
  .project({field: 1})  Project fields
  .explain()            Explain query plan

${colors.cyan}Other Commands:${colors.reset}
  it                    Iterate to next batch of results
  print("msg")          Print a message
  printjson(obj)        Print JSON object
  load("file.js")       Load and execute a script
  version()             Show server version
  help                  Show this help message
  clear / cls           Clear the screen
  exit / quit           Exit the shell
`);
}

// ============================================================================
// Database Commands
// ============================================================================

/**
 * List all databases.
 */
export async function showDatabases(state: ShellState): Promise<void> {
  const databases = await state.lake.listDatabases();

  if (databases.length === 0) {
    printInfo('No databases found');
    return;
  }

  const colors = getExtendedColors();
  console.log(`${colors.bright}Databases:${colors.reset}`);
  for (const db of databases) {
    console.log(`  ${db}`);
  }
}

/**
 * List collections in the current database.
 */
export async function showCollections(state: ShellState): Promise<void> {
  const db = state.lake.db(state.currentDb);
  const collections = await db.listCollections();
  state.collections = collections;

  if (collections.length === 0) {
    printInfo(`No collections in database '${state.currentDb}'`);
    return;
  }

  const colors = getExtendedColors();
  console.log(`${colors.bright}Collections in '${state.currentDb}':${colors.reset}`);
  for (const coll of collections) {
    console.log(`  ${coll}`);
  }
}

/**
 * Switch to a different database.
 */
export function useDatabase(state: ShellState, dbName: string): void {
  state.currentDb = dbName;
  state.collections = []; // Clear cached collections
  printSuccess(`switched to db ${dbName}`);
}

// ============================================================================
// Collection Method Execution
// ============================================================================

/**
 * Execute a collection method chain (e.g., find().sort().limit()).
 */
export async function executeMethodChain(
  state: ShellState,
  collection: string,
  methodChain: string
): Promise<void> {
  const db = state.lake.db(state.currentDb);
  const coll = db.collection(collection);

  const methods = parseMethodChain(methodChain);
  if (methods.length === 0) {
    printError(`Invalid method chain: ${methodChain}`);
    return;
  }

  const [firstMethod, ...chainedMethods] = methods;

  // Parse first method arguments
  let args: unknown[];
  try {
    args = parseArgs(firstMethod!.args);
  } catch (error) {
    printError((error as Error).message);
    console.log(`syntax error at column ${firstMethod!.args.indexOf('}')}`);
    return;
  }

  try {
    await executeCollectionMethod(state, coll, db, collection, firstMethod!, chainedMethods, args);
  } catch (error) {
    handleExecutionError(state, error as Error);
  }
}

/**
 * Execute a specific collection method.
 */
async function executeCollectionMethod(
  state: ShellState,
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>,
  db: ReturnType<MongoLake['db']>,
  collectionName: string,
  method: ParsedMethod,
  chainedMethods: ParsedMethod[],
  args: unknown[]
): Promise<void> {
  switch (method.method) {
    case 'find':
      await executeFindWithChain(state, coll, args, chainedMethods);
      break;

    case 'findOne':
      await executeFindOne(coll, args);
      break;

    case 'insertOne':
      await executeInsertOne(coll, args);
      break;

    case 'insertMany':
      await executeInsertMany(coll, args);
      break;

    case 'updateOne':
      await executeUpdateOne(coll, args);
      break;

    case 'updateMany':
      await executeUpdateMany(coll, args);
      break;

    case 'replaceOne':
      await executeReplaceOne(coll, args);
      break;

    case 'deleteOne':
      await executeDeleteOne(coll, args);
      break;

    case 'deleteMany':
      await executeDeleteMany(coll, args);
      break;

    case 'countDocuments':
      await executeCountDocuments(coll, args);
      break;

    case 'distinct':
      await executeDistinct(coll, args);
      break;

    case 'aggregate':
      await executeAggregate(coll, args);
      break;

    case 'createIndex':
      await executeCreateIndex(coll, args);
      break;

    case 'getIndexes':
      await executeGetIndexes(coll);
      break;

    case 'dropIndex':
      await executeDropIndex(coll, args);
      break;

    case 'stats':
      await executeStats(coll);
      break;

    case 'bulkWrite':
      await executeBulkWrite(coll, args);
      break;

    case 'drop':
      await executeDrop(db, collectionName);
      break;

    default:
      printError(`Unknown method: ${method.method}`);
  }
}

// ============================================================================
// Find with Cursor Chaining
// ============================================================================

/**
 * Execute find() with optional cursor method chaining.
 */
async function executeFindWithChain(
  state: ShellState,
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>,
  args: unknown[],
  chainedMethods: ParsedMethod[]
): Promise<void> {
  const filter = (args[0] as Filter<Document>) || {};
  let cursor = coll.find(filter);

  // Apply chained cursor methods
  for (const chained of chainedMethods) {
    const chainedArgs = parseArgs(chained.args);

    switch (chained.method) {
      case 'limit':
        cursor = (cursor as { limit: (n: number) => typeof cursor }).limit(chainedArgs[0] as number);
        break;
      case 'skip':
        cursor = (cursor as { skip: (n: number) => typeof cursor }).skip(chainedArgs[0] as number);
        break;
      case 'sort':
        cursor = (cursor as { sort: (spec: Record<string, number>) => typeof cursor }).sort(
          chainedArgs[0] as Record<string, number>
        );
        break;
      case 'project':
        cursor = (cursor as { project: (spec: Record<string, number>) => typeof cursor }).project(
          chainedArgs[0] as Record<string, number>
        );
        break;
      case 'explain': {
        if (hasExplain(cursor)) {
          const explainResult = await cursor.explain();
          console.log(highlightOutput(formatOutput(explainResult)));
        } else {
          printError('Cursor does not support explain()');
        }
        return;
      }
      case 'toArray':
        // Will be handled below
        break;
    }
  }

  // Execute and display results
  const docs = await cursor.toArray();
  state.lastCursor = cursor as AsyncCursor;

  if (docs.length === 0) {
    printInfo('No documents found');
  } else {
    const displayDocs = docs.slice(0, state.shellBatchSize);
    for (const doc of displayDocs) {
      console.log(highlightOutput(formatOutput(doc)));
    }
    if (docs.length > state.shellBatchSize) {
      printInfo(`Type "it" for more`);
    } else {
      printInfo(`${docs.length} document(s) found`);
    }
  }
}

// ============================================================================
// Individual Method Handlers
// ============================================================================

async function executeFindOne(
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>,
  args: unknown[]
): Promise<void> {
  const filter = (args[0] as Filter<Document>) || {};
  const doc = await coll.findOne(filter);
  if (doc) {
    console.log(highlightOutput(formatOutput(doc)));
  } else {
    printInfo('No document found');
  }
}

async function executeInsertOne(
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>,
  args: unknown[]
): Promise<void> {
  if (args.length === 0) {
    printError('insertOne requires a document argument');
    return;
  }
  const doc = args[0] as Document;
  const result = await coll.insertOne(doc);
  console.log(highlightOutput(formatOutput({
    acknowledged: result.acknowledged,
    insertedId: result.insertedId,
  })));
}

async function executeInsertMany(
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>,
  args: unknown[]
): Promise<void> {
  if (args.length === 0) {
    printError('insertMany requires an array of documents');
    return;
  }
  const docs = args[0] as Document[];
  if (!Array.isArray(docs)) {
    printError('insertMany requires an array of documents');
    return;
  }
  const result = await coll.insertMany(docs);
  console.log(highlightOutput(formatOutput({
    acknowledged: result.acknowledged,
    insertedCount: result.insertedCount,
    insertedIds: result.insertedIds,
  })));
}

async function executeUpdateOne(
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>,
  args: unknown[]
): Promise<void> {
  if (args.length < 2) {
    printError('updateOne requires filter and update arguments');
    return;
  }
  const filter = args[0] as Filter<Document>;
  const update = args[1] as Update<Document>;
  const result = await coll.updateOne(filter, update);
  console.log(highlightOutput(formatOutput({
    acknowledged: result.acknowledged,
    matchedCount: result.matchedCount,
    modifiedCount: result.modifiedCount,
  })));
}

async function executeUpdateMany(
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>,
  args: unknown[]
): Promise<void> {
  if (args.length < 2) {
    printError('updateMany requires filter and update arguments');
    return;
  }
  const filter = args[0] as Filter<Document>;
  const update = args[1] as Update<Document>;
  const result = await coll.updateMany(filter, update);
  console.log(highlightOutput(formatOutput({
    acknowledged: result.acknowledged,
    matchedCount: result.matchedCount,
    modifiedCount: result.modifiedCount,
  })));
}

async function executeReplaceOne(
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>,
  args: unknown[]
): Promise<void> {
  if (args.length < 2) {
    printError('replaceOne requires filter and replacement arguments');
    return;
  }
  if (!hasExtendedMethod(coll, 'replaceOne')) {
    printError('Collection does not support replaceOne()');
    return;
  }
  const filter = args[0] as Filter<Document>;
  const replacement = args[1] as Document;
  const result = await coll.replaceOne(filter, replacement);
  console.log(highlightOutput(formatOutput({
    acknowledged: result.acknowledged,
    matchedCount: result.matchedCount,
    modifiedCount: result.modifiedCount,
  })));
}

async function executeDeleteOne(
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>,
  args: unknown[]
): Promise<void> {
  const filter = (args[0] as Filter<Document>) || {};
  const result = await coll.deleteOne(filter);
  console.log(highlightOutput(formatOutput({
    acknowledged: result.acknowledged,
    deletedCount: result.deletedCount,
  })));
}

async function executeDeleteMany(
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>,
  args: unknown[]
): Promise<void> {
  const filter = (args[0] as Filter<Document>) || {};
  const result = await coll.deleteMany(filter);
  console.log(highlightOutput(formatOutput({
    acknowledged: result.acknowledged,
    deletedCount: result.deletedCount,
  })));
}

async function executeCountDocuments(
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>,
  args: unknown[]
): Promise<void> {
  const filter = (args[0] as Filter<Document>) || {};
  const count = await coll.countDocuments(filter);
  console.log(count);
}

async function executeDistinct(
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>,
  args: unknown[]
): Promise<void> {
  const field = args[0] as string;
  if (!hasExtendedMethod(coll, 'distinct')) {
    printError('Collection does not support distinct()');
    return;
  }
  const result = await coll.distinct(field);
  console.log(highlightOutput(formatOutput(result)));
}

async function executeAggregate(
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>,
  args: unknown[]
): Promise<void> {
  if (args.length === 0) {
    printError('aggregate requires a pipeline array');
    return;
  }
  const pipeline = args[0] as AggregationStage[];
  if (!Array.isArray(pipeline)) {
    printError('aggregate requires a pipeline array');
    return;
  }
  const results = await coll.aggregate(pipeline).toArray();
  if (results.length === 0) {
    printInfo('No results');
  } else {
    for (const doc of results) {
      console.log(highlightOutput(formatOutput(doc)));
    }
    printInfo(`${results.length} result(s)`);
  }
}

async function executeCreateIndex(
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>,
  args: unknown[]
): Promise<void> {
  if (args.length === 0) {
    printError('createIndex requires an index specification');
    return;
  }
  if (!hasExtendedMethod(coll, 'createIndex')) {
    printError('Collection does not support createIndex()');
    return;
  }
  const indexSpec = args[0] as Record<string, number>;
  const indexName = await coll.createIndex(indexSpec);
  console.log(highlightOutput(formatOutput(indexName)));
}

async function executeGetIndexes(
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>
): Promise<void> {
  if (!hasExtendedMethod(coll, 'listIndexes')) {
    printError('Collection does not support listIndexes()');
    return;
  }
  const indexes = await coll.listIndexes();
  console.log(highlightOutput(formatOutput(indexes)));
}

async function executeDropIndex(
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>,
  args: unknown[]
): Promise<void> {
  if (!hasExtendedMethod(coll, 'dropIndex')) {
    printError('Collection does not support dropIndex()');
    return;
  }
  const indexName = args[0] as string;
  const result = await coll.dropIndex(indexName);
  console.log(highlightOutput(formatOutput(result)));
}

async function executeStats(
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>
): Promise<void> {
  if (!hasExtendedMethod(coll, 'stats')) {
    printError('Collection does not support stats()');
    return;
  }
  const result = await coll.stats();
  console.log(highlightOutput(formatOutput(result)));
}

async function executeBulkWrite(
  coll: ReturnType<ReturnType<MongoLake['db']>['collection']>,
  args: unknown[]
): Promise<void> {
  if (args.length === 0) {
    printError('bulkWrite requires an operations array');
    return;
  }
  if (!hasExtendedMethod(coll, 'bulkWrite')) {
    printError('Collection does not support bulkWrite()');
    return;
  }
  const operations = args[0] as unknown[];
  const result = await coll.bulkWrite(operations);
  console.log(highlightOutput(formatOutput(result)));
}

async function executeDrop(
  db: ReturnType<MongoLake['db']>,
  collectionName: string
): Promise<void> {
  await db.dropCollection(collectionName);
  printSuccess(`Collection '${collectionName}' dropped`);
}

// ============================================================================
// Error Handling
// ============================================================================

/**
 * Handle execution errors with helpful messages.
 */
function handleExecutionError(state: ShellState, error: Error): void {
  const errorMessage = error.message;

  // Unknown operator error
  if (errorMessage.includes('Unknown operator')) {
    printError(`${errorMessage}. Supported operators: $eq, $gt, $gte, $lt, $lte, $ne, $in, $nin, $and, $or, $not`);
    return;
  }

  // Duplicate key error
  if (errorMessage.includes('duplicate key')) {
    printError(errorMessage);
    return;
  }

  // Validation error
  if (errorMessage.includes('validation') || errorMessage.includes('required')) {
    printError(errorMessage);
    return;
  }

  // Connection lost - attempt reconnect
  if (errorMessage.includes('Connection lost')) {
    handleConnectionLost(state);
    return;
  }

  // Generic error
  printError(errorMessage);
}

/**
 * Type guard for connectable lake instance
 */
function hasConnect(lake: unknown): lake is { connect: (opts?: unknown) => Promise<void> } {
  return typeof (lake as Record<string, unknown>).connect === 'function';
}

/**
 * Handle connection lost and attempt reconnect.
 */
async function handleConnectionLost(state: ShellState): Promise<void> {
  printInfo('Connection lost, attempting to reconnect...');
  try {
    if (hasConnect(state.lake)) {
      await state.lake.connect();
      printSuccess('Reconnected');
    } else {
      printError('Lake does not support reconnection');
    }
  } catch {
    printError('Could not reconnect');
  }
}

// ============================================================================
// Utility Functions for print/printjson
// ============================================================================

/**
 * Handle print() command - evaluates simple strings.
 */
export function handlePrint(arg: string): void {
  const stringMatch = arg.match(/^["'](.*)["']$/);
  if (stringMatch) {
    console.log(stringMatch[1]);
  } else {
    console.log(arg);
  }
}

/**
 * Handle printjson() command - parses and formats JSON.
 */
export function handlePrintJson(arg: string): void {
  try {
    let parsed: unknown;
    try {
      parsed = JSON.parse(arg);
    } catch {
      parsed = JSON.parse(jsToJson(arg));
    }
    console.log(highlightOutput(formatOutput(parsed)));
  } catch {
    console.log(arg);
  }
}
