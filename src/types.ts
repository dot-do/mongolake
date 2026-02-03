/**
 * MongoLake Types
 *
 * MongoDB-compatible type definitions
 */

// ============================================================================
// Branded Types
// ============================================================================

// Branded types for stronger type safety
declare const __brand: unique symbol;
type Brand<T, B> = T & { readonly [__brand]: B };

export type DocumentId = Brand<string, 'DocumentId'>;
export type ShardId = Brand<number, 'ShardId'>;
export type CollectionName = Brand<string, 'CollectionName'>;
export type DatabaseName = Brand<string, 'DatabaseName'>;

// Helper functions to create branded types
export function toDocumentId(id: string): DocumentId {
  return id as DocumentId;
}
export function toShardId(id: number): ShardId {
  return id as ShardId;
}
export function toCollectionName(name: string): CollectionName {
  return name as CollectionName;
}
export function toDatabaseName(name: string): DatabaseName {
  return name as DatabaseName;
}

// ============================================================================
// Branded Type Runtime Validation
// ============================================================================

/**
 * Type guard to check if a value is a valid DocumentId.
 * DocumentIds must be non-empty strings.
 *
 * @param value - The value to check
 * @returns True if value is a valid DocumentId
 */
export function isDocumentId(value: unknown): value is DocumentId {
  return typeof value === 'string' && value.length > 0;
}

/**
 * Assert that a value is a valid DocumentId, throwing if not.
 *
 * @param value - The value to assert
 * @param message - Optional error message
 * @throws TypeError if value is not a valid DocumentId
 */
export function assertDocumentId(value: unknown, message?: string): asserts value is DocumentId {
  if (!isDocumentId(value)) {
    throw new TypeError(message ?? 'Value is not a valid DocumentId: must be a non-empty string');
  }
}

/**
 * Type guard to check if a value is a valid ShardId.
 * ShardIds must be non-negative integers.
 *
 * @param value - The value to check
 * @returns True if value is a valid ShardId
 */
export function isShardId(value: unknown): value is ShardId {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0;
}

/**
 * Assert that a value is a valid ShardId, throwing if not.
 *
 * @param value - The value to assert
 * @param message - Optional error message
 * @throws TypeError if value is not a valid ShardId
 */
export function assertShardId(value: unknown, message?: string): asserts value is ShardId {
  if (!isShardId(value)) {
    throw new TypeError(message ?? 'Value is not a valid ShardId: must be a non-negative integer');
  }
}

/**
 * Valid collection name pattern.
 * MongoDB collection names:
 * - Cannot be empty
 * - Cannot contain null characters
 * - Cannot start with 'system.' (reserved)
 * - Cannot contain '$' (reserved for operators)
 * - Maximum length of 120 bytes (we check characters, assuming ASCII)
 */
const COLLECTION_NAME_PATTERN = /^(?!system\.)[^$\x00]{1,120}$/;

/**
 * Type guard to check if a value is a valid CollectionName.
 * CollectionNames must follow MongoDB naming conventions.
 *
 * @param value - The value to check
 * @returns True if value is a valid CollectionName
 */
export function isCollectionName(value: unknown): value is CollectionName {
  return typeof value === 'string' && COLLECTION_NAME_PATTERN.test(value);
}

/**
 * Assert that a value is a valid CollectionName, throwing if not.
 *
 * @param value - The value to assert
 * @param message - Optional error message
 * @throws TypeError if value is not a valid CollectionName
 */
export function assertCollectionName(value: unknown, message?: string): asserts value is CollectionName {
  if (!isCollectionName(value)) {
    throw new TypeError(
      message ?? 'Value is not a valid CollectionName: must be 1-120 characters, cannot start with "system.", cannot contain "$" or null characters'
    );
  }
}

/**
 * Valid database name pattern.
 * MongoDB database names:
 * - Cannot be empty
 * - Cannot contain /\. "$*<>:|?
 * - Cannot contain null characters
 * - Maximum length of 64 bytes (we check characters, assuming ASCII)
 */
const DATABASE_NAME_PATTERN = /^[^/\\. "$*<>:|?\x00]{1,64}$/;

/**
 * Type guard to check if a value is a valid DatabaseName.
 * DatabaseNames must follow MongoDB naming conventions.
 *
 * @param value - The value to check
 * @returns True if value is a valid DatabaseName
 */
export function isDatabaseName(value: unknown): value is DatabaseName {
  return typeof value === 'string' && DATABASE_NAME_PATTERN.test(value);
}

/**
 * Assert that a value is a valid DatabaseName, throwing if not.
 *
 * @param value - The value to assert
 * @param message - Optional error message
 * @throws TypeError if value is not a valid DatabaseName
 */
export function assertDatabaseName(value: unknown, message?: string): asserts value is DatabaseName {
  if (!isDatabaseName(value)) {
    throw new TypeError(
      message ?? 'Value is not a valid DatabaseName: must be 1-64 characters, cannot contain /\\. "$*<>:|? or null characters'
    );
  }
}

// ============================================================================
// Document Types
// ============================================================================

/** Any valid BSON value */
export type BSONValue =
  | string
  | number
  | boolean
  | null
  | Date
  | Uint8Array
  | ObjectId
  | BSONValue[]
  | { [key: string]: BSONValue };

// Module-level state for ObjectId generation (initialized once per process)
const counterRandomBytes = new Uint8Array(3);
crypto.getRandomValues(counterRandomBytes);
const objectIdState = {
  // 5-byte random value, generated once per process
  randomBytes: crypto.getRandomValues(new Uint8Array(5)),
  // 3-byte counter, initialized to a random value and incremented for each ObjectId
  counter: (counterRandomBytes[0]! << 16) | (counterRandomBytes[1]! << 8) | counterRandomBytes[2]!,
};

/** MongoDB ObjectId */
export class ObjectId {
  private readonly bytes: Uint8Array;

  constructor(id?: string | Uint8Array) {
    if (id instanceof Uint8Array) {
      this.bytes = id;
    } else if (typeof id === 'string') {
      this.bytes = ObjectId.fromHex(id);
    } else {
      this.bytes = ObjectId.generate();
    }
  }

  private static fromHex(hex: string): Uint8Array {
    const bytes = new Uint8Array(12);
    for (let i = 0; i < 12; i++) {
      bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    }
    return bytes;
  }

  private static generate(): Uint8Array {
    const bytes = new Uint8Array(12);
    // Timestamp (4 bytes)
    const timestamp = Math.floor(Date.now() / 1000);
    bytes[0] = (timestamp >> 24) & 0xff;
    bytes[1] = (timestamp >> 16) & 0xff;
    bytes[2] = (timestamp >> 8) & 0xff;
    bytes[3] = timestamp & 0xff;
    // Random value (5 bytes) - same for all ObjectIds in this process
    bytes[4] = objectIdState.randomBytes[0]!;
    bytes[5] = objectIdState.randomBytes[1]!;
    bytes[6] = objectIdState.randomBytes[2]!;
    bytes[7] = objectIdState.randomBytes[3]!;
    bytes[8] = objectIdState.randomBytes[4]!;
    // Counter (3 bytes) - incrementing, wraps at 0xffffff
    const counter = objectIdState.counter;
    objectIdState.counter = (objectIdState.counter + 1) & 0xffffff;
    bytes[9] = (counter >> 16) & 0xff;
    bytes[10] = (counter >> 8) & 0xff;
    bytes[11] = counter & 0xff;
    return bytes;
  }

  toString(): string {
    return Array.from(this.bytes)
      .map((b) => b.toString(16).padStart(2, '0'))
      .join('');
  }

  toHexString(): string {
    return this.toString();
  }

  getTimestamp(): Date {
    const timestamp =
      (this.bytes[0]! << 24) |
      (this.bytes[1]! << 16) |
      (this.bytes[2]! << 8) |
      this.bytes[3]!;
    return new Date(timestamp * 1000);
  }

  equals(other: ObjectId): boolean {
    return this.toString() === other.toString();
  }

  static isValid(id: string): boolean {
    return /^[0-9a-fA-F]{24}$/.test(id);
  }
}

/** Base document with optional _id */
export interface Document {
  _id?: string | ObjectId;
  [key: string]: BSONValue | undefined;
}

/** Document with required _id (uses intersection to preserve T's type information) */
export type WithId<T> = T & { _id: string | ObjectId };

/**
 * Looser document type for internal operations where we need to work with
 * arbitrary objects that may not strictly conform to BSONValue constraints.
 * Use this when interfacing with external data or when type strictness is not required.
 */
export type AnyDocument = {
  _id?: string | ObjectId | unknown;
  [key: string]: unknown;
};

/**
 * Type guard to check if a value is a valid Document.
 * Validates that the value is a non-null object. The _id field
 * is optional, but if present must be a string or ObjectId.
 *
 * @param value - The value to check
 * @returns True if value is a valid Document
 */
export function isDocument(value: unknown): value is Document {
  if (value === null || typeof value !== 'object') {
    return false;
  }
  const doc = value as Record<string, unknown>;
  // _id is optional, but if present must be a string or ObjectId
  if (doc._id !== undefined) {
    if (typeof doc._id !== 'string' && !(doc._id instanceof ObjectId)) {
      return false;
    }
  }
  return true;
}

/**
 * Assert that a value is a Document, throwing if not.
 * Useful for runtime validation at API boundaries.
 *
 * @param value - The value to assert
 * @param message - Optional error message
 * @throws TypeError if value is not a valid Document
 */
export function assertDocument(value: unknown, message?: string): asserts value is Document {
  if (!isDocument(value)) {
    throw new TypeError(message ?? 'Value is not a valid Document');
  }
}

/**
 * Convert an AnyDocument or Record<string, unknown> to Document type.
 * This is a type-safe way to cast looser document types to Document
 * for use with strongly-typed functions like matchesFilter.
 *
 * Note: This is a compile-time cast only, no runtime conversion occurs.
 * Use isDocument() or assertDocument() for runtime validation.
 */
export function asDocument<T extends AnyDocument>(doc: T): T & Document {
  return doc as T & Document;
}

// ============================================================================
// Semantic Type Wrappers
// ============================================================================

/**
 * A generic object representing arbitrary MongoDB document fields.
 * Use this instead of `Record<string, unknown>` for better type semantics.
 * This type indicates the value is a document-like structure with string keys.
 */
export type DocumentFields = { [key: string]: unknown };

/**
 * Type representing the raw value of a comparison operator condition.
 * Used internally when processing filter conditions.
 */
export type OperatorCondition = { [operator: string]: unknown };

/**
 * Type representing a MongoDB filter query in its raw form.
 * This is used when the document type is not known or when processing
 * filters generically. For strongly-typed filters, use Filter<T>.
 */
export type FilterQuery = DocumentFields;

/**
 * Type representing MongoDB update operators in raw form.
 * This is used when the document type is not known or when processing
 * updates generically. For strongly-typed updates, use Update<T>.
 */
export type UpdateQuery = {
  $set?: DocumentFields;
  $unset?: DocumentFields;
  $setOnInsert?: DocumentFields;
  $inc?: { [key: string]: number };
  $mul?: { [key: string]: number };
  $min?: DocumentFields;
  $max?: DocumentFields;
  $rename?: { [key: string]: string };
  $currentDate?: { [key: string]: true | { $type: 'date' | 'timestamp' } };
  $push?: DocumentFields;
  $pull?: DocumentFields;
  $addToSet?: DocumentFields;
  $pop?: { [key: string]: 1 | -1 };
};

/**
 * Type representing MongoDB projection in raw form.
 * Keys are field paths, values are 0 (exclude) or 1 (include).
 */
export type ProjectionQuery = { [key: string]: 0 | 1 };

/**
 * Type representing MongoDB sort specification in raw form.
 * Keys are field paths, values are 1 (ascending) or -1 (descending).
 */
export type SortQuery = { [key: string]: 1 | -1 };

/**
 * Type representing connection string options.
 * Used for parsed options from MongoDB connection strings.
 */
export type ConnectionOptions = { [key: string]: unknown };

/**
 * Type guard to check if a value is a DocumentFields (plain object).
 * Returns true for non-null objects that are not arrays.
 */
export function isDocumentFields(value: unknown): value is DocumentFields {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/**
 * Type guard to check if a value is an operator condition (has $ keys).
 */
export function isOperatorCondition(value: unknown): value is OperatorCondition {
  if (!isDocumentFields(value)) return false;
  return Object.keys(value).some(key => key.startsWith('$'));
}

/**
 * Convert an unknown value to DocumentFields with runtime check.
 * Throws if the value is not a valid object.
 */
export function toDocumentFields(value: unknown, fieldName = 'value'): DocumentFields {
  if (!isDocumentFields(value)) {
    throw new TypeError(`${fieldName} must be an object, got ${value === null ? 'null' : typeof value}`);
  }
  return value;
}

/**
 * Type guard to check if a value is a valid FilterQuery.
 * A FilterQuery is a plain object with string keys.
 */
export function isFilterQuery(value: unknown): value is FilterQuery {
  return isDocumentFields(value);
}

/**
 * Type guard to check if a value is a valid UpdateQuery.
 * Checks that the value is an object and has at least one update operator.
 */
export function isUpdateQuery(value: unknown): value is UpdateQuery {
  if (!isDocumentFields(value)) return false;
  const updateOps = ['$set', '$unset', '$setOnInsert', '$inc', '$mul', '$min', '$max', '$rename', '$currentDate', '$push', '$pull', '$addToSet', '$pop'];
  return updateOps.some(op => op in value);
}

// ============================================================================
// Filter Types
// ============================================================================

/** Comparison operators */
export interface ComparisonOperators<T> {
  $eq?: T;
  $ne?: T;
  $gt?: T;
  $gte?: T;
  $lt?: T;
  $lte?: T;
  $in?: T[];
  $nin?: T[];
}

/** Element operators */
export interface ElementOperators {
  $exists?: boolean;
  $type?: string | number;
}

/** Array operators - T is the field type (which may be an array) */
export type ArrayOperators<T> = T extends (infer U)[]
  ? {
      /** Match all elements - expects array of element type */
      $all?: U[];
      /** Match element within array - filter on element type */
      $elemMatch?: U extends Document ? Filter<U> : FilterQuery;
      $size?: number;
    }
  : never;

/** Filter condition for a field */
export type FilterCondition<T> =
  | T
  | ComparisonOperators<T>
  | ElementOperators
  | ArrayOperators<T>;

/** Logical operators - T must extend Document for Filter<T> compatibility */
export interface LogicalOperators<T extends Document> {
  $and?: Filter<T>[];
  $or?: Filter<T>[];
  $nor?: Filter<T>[];
  $not?: Filter<T>;
}

/** Root filter type */
export type Filter<T extends Document = Document> = {
  [K in keyof T]?: FilterCondition<T[K]>;
} & LogicalOperators<T> & {
  $text?: { $search: string; $language?: string };
  $where?: string | ((this: T) => boolean);
};

// ============================================================================
// Update Types
// ============================================================================

/** Field update operators */
export interface UpdateOperators<T extends Document> {
  $set?: Partial<T>;
  $unset?: { [K in keyof T]?: '' | 1 | true };
  $setOnInsert?: Partial<T>;
  $inc?: { [K in keyof T]?: number };
  $mul?: { [K in keyof T]?: number };
  $min?: Partial<T>;
  $max?: Partial<T>;
  $rename?: { [K in keyof T]?: string };
  $currentDate?: { [K in keyof T]?: true | { $type: 'date' | 'timestamp' } };
}

/** Array update operators */
export interface ArrayUpdateOperators<T extends Document> {
  $push?: { [K in keyof T]?: T[K] extends (infer U)[] ? U | { $each: U[] } : never };
  $pull?: { [K in keyof T]?: T[K] extends (infer U)[] ? U | Filter<U & Document> : never };
  $addToSet?: { [K in keyof T]?: T[K] extends (infer U)[] ? U | { $each: U[] } : never };
  $pop?: { [K in keyof T]?: 1 | -1 };
}

/** Update document */
export type Update<T extends Document> = UpdateOperators<T> & ArrayUpdateOperators<T>;

// ============================================================================
// Aggregation Types
// ============================================================================

/** Aggregation pipeline stages */
export type AggregationStage =
  | { $match: Filter<Document> }
  | { $group: GroupStage }
  | { $sort: { [key: string]: 1 | -1 } }
  | { $limit: number }
  | { $skip: number }
  | { $project: { [key: string]: 0 | 1 | ProjectionExpression } }
  | { $unwind: string | { path: string; preserveNullAndEmptyArrays?: boolean } }
  | { $lookup: LookupStage }
  | { $count: string }
  | { $addFields: { [key: string]: unknown } }
  | { $set: { [key: string]: unknown } }
  | { $unset: string | string[] }
  | { $facet: FacetStage }
  | { $bucket: BucketStage }
  | { $bucketAuto: BucketAutoStage }
  | { $graphLookup: GraphLookupStage }
  | { $merge: MergeStage }
  | { $out: OutStage | string }
  | { $redact: RedactExpression }
  | { $replaceRoot: ReplaceRootStage }
  | { $replaceWith: string | { [key: string]: unknown } }
  | { $sample: SampleStage }
  | { $sortByCount: string };

/** $facet stage - run multiple pipelines in parallel */
export interface FacetStage {
  [outputField: string]: AggregationStage[];
}

/** $bucket stage - group documents into buckets based on boundaries */
export interface BucketStage {
  /** Field path or expression to group by */
  groupBy: string;
  /** Array of boundary values that define the bucket boundaries */
  boundaries: (number | Date)[];
  /** Literal value for documents that fall outside all boundaries */
  default?: string;
  /** Accumulator expressions for each bucket */
  output?: { [field: string]: unknown };
}

/** $bucketAuto stage - automatically distribute documents into buckets */
export interface BucketAutoStage {
  /** Field path or expression to group by */
  groupBy: string;
  /** Number of buckets to create */
  buckets: number;
  /** Accumulator expressions for each bucket */
  output?: { [field: string]: unknown };
  /** Granularity for bucket boundaries */
  granularity?: 'R5' | 'R10' | 'R20' | 'R40' | 'R80' | '1-2-5' | 'E6' | 'E12' | 'E24' | 'E48' | 'E96' | 'E192' | 'POWERSOF2';
}

/** $graphLookup stage - recursive graph traversal */
export interface GraphLookupStage {
  /** Collection to search */
  from: string;
  /** Expression that specifies the value to start the search with */
  startWith: string;
  /** Field in the documents of the from collection to match */
  connectFromField: string;
  /** Field in the input documents to match */
  connectToField: string;
  /** Name of the array field to add to the input documents */
  as: string;
  /** Maximum recursion depth */
  maxDepth?: number;
  /** Name of the field to add to each traversed document */
  depthField?: string;
  /** Additional query to filter documents */
  restrictSearchWithMatch?: Filter<Document>;
}

/** $merge stage - merge documents into a collection */
export interface MergeStage {
  /** Target collection */
  into: string | { db?: string; coll: string };
  /** Field(s) to match on */
  on?: string | string[];
  /** Variables to use in the update pipeline */
  let?: { [key: string]: unknown };
  /** Action when document matches */
  whenMatched?: 'replace' | 'keepExisting' | 'merge' | 'fail' | unknown[];
  /** Action when document does not match */
  whenNotMatched?: 'insert' | 'discard' | 'fail';
}

/** $out stage - write documents to a collection */
export interface OutStage {
  /** Target database */
  db?: string;
  /** Target collection */
  coll: string;
}

/** $redact expression - access control at document level */
export type RedactExpression =
  | '$$DESCEND'
  | '$$PRUNE'
  | '$$KEEP'
  | { $cond: { if: unknown; then: RedactExpression; else: RedactExpression } };

/** $replaceRoot stage - replace document with new root */
export interface ReplaceRootStage {
  /** The new root document expression */
  newRoot: string | { [key: string]: unknown };
}

/** $sample stage - random sampling */
export interface SampleStage {
  /** Number of documents to sample */
  size: number;
}

/** Accumulator expressions for $group */
export type AccumulatorExpression =
  | { $sum: number | string }
  | { $avg: string }
  | { $min: string }
  | { $max: string }
  | { $first: string }
  | { $last: string }
  | { $push: string }
  | { $addToSet: string }
  | { $count: Record<string, never> };

/** Group stage specification */
export interface GroupStage {
  _id: unknown;
  [key: string]: unknown;
}

/** Projection expression */
export type ProjectionExpression = unknown;

/** Lookup stage */
export interface LookupStage {
  from: string;
  localField?: string;
  foreignField?: string;
  as: string;
  let?: { [key: string]: unknown };
  pipeline?: AggregationStage[];
}

// ============================================================================
// Result Types
// ============================================================================

/** Insert result */
export interface InsertOneResult {
  acknowledged: boolean;
  insertedId: string | ObjectId;
}

export interface InsertManyResult {
  acknowledged: boolean;
  insertedCount: number;
  insertedIds: { [key: number]: string | ObjectId };
}

/** Update result */
export interface UpdateResult {
  acknowledged: boolean;
  matchedCount: number;
  modifiedCount: number;
  upsertedCount: number;
  upsertedId?: string | ObjectId;
}

/** Delete result */
export interface DeleteResult {
  acknowledged: boolean;
  deletedCount: number;
}

/** Bulk write result */
export interface BulkWriteResult {
  acknowledged: boolean;
  insertedCount: number;
  matchedCount: number;
  modifiedCount: number;
  deletedCount: number;
  upsertedCount: number;
  insertedIds: { [key: number]: string | ObjectId };
  upsertedIds: { [key: number]: string | ObjectId };
}

// ============================================================================
// Corruption Report Types
// ============================================================================

/**
 * Report of a corrupted file encountered during read operations.
 * Used to track data loss when skipCorruptedFiles is enabled.
 */
export interface CorruptionReport {
  /** Path or name of the corrupted file */
  filename: string;
  /** Error message describing the corruption */
  error: string;
  /** Timestamp when the corruption was detected */
  timestamp: Date;
  /** Collection the file belongs to */
  collection: string;
  /** Database the collection belongs to */
  database?: string;
}

/**
 * Callback function called when a corrupted file is skipped.
 * Allows callers to handle corruption events in real-time.
 */
export type CorruptionCallback = (report: CorruptionReport) => void;

/**
 * Query result metadata including corruption information.
 * Returned alongside results when corruption tracking is enabled.
 */
export interface QueryMetadata {
  /** List of corrupted files encountered during the query */
  corruptedFiles: CorruptionReport[];
  /** Total number of files processed (including corrupted ones) */
  totalFilesProcessed: number;
  /** Number of files that were skipped due to corruption */
  skippedCount: number;
  /** Whether any data loss occurred */
  hasDataLoss: boolean;
}

// ============================================================================
// Options Types
// ============================================================================

/**
 * Session option type for collection operations.
 * Using generic interface to avoid circular dependencies with session module.
 */
export interface SessionOption {
  /**
   * Client session for transaction support.
   * When provided, the operation will be part of the session's transaction.
   */
  session?: {
    id: string;
    inTransaction: boolean;
    bufferOperation: (op: unknown) => void;
  };
}

/** Find options */
export interface FindOptions extends SessionOption {
  projection?: { [key: string]: 0 | 1 };
  sort?: { [key: string]: 1 | -1 };
  limit?: number;
  skip?: number;
  hint?: string | { [key: string]: 1 | -1 };
  maxTimeMS?: number;
  /**
   * If true, skip corrupted Parquet files instead of failing.
   * Warning: This may cause data loss. Use onCorruptedFile callback
   * or check metadata.corruptedFiles to audit skipped files.
   * @default false
   */
  skipCorruptedFiles?: boolean;
  /**
   * Callback invoked when a corrupted file is skipped.
   * Only called when skipCorruptedFiles is true.
   * Use this for real-time logging or alerting on data corruption.
   */
  onCorruptedFile?: CorruptionCallback;
  /**
   * If true, use query result cache if available.
   * Cached results are returned for identical queries within the TTL.
   * Cache is automatically invalidated on writes to the collection.
   * @default true
   */
  useCache?: boolean;
  /**
   * If true, bypass the cache and always execute the query.
   * The result will still be cached for future queries.
   * @default false
   */
  noCache?: boolean;
}

/** Update options */
export interface UpdateOptions extends SessionOption {
  upsert?: boolean;
  arrayFilters?: Filter<Document>[];
  hint?: string | { [key: string]: 1 | -1 };
}

/** Delete options */
export interface DeleteOptions extends SessionOption {
  hint?: string | { [key: string]: 1 | -1 };
}

/** Aggregate options */
export interface AggregateOptions extends SessionOption {
  allowDiskUse?: boolean;
  maxTimeMS?: number;
  hint?: string | { [key: string]: 1 | -1 };
  /**
   * Enable distributed aggregation across shards.
   * When enabled, the aggregation pipeline will be analyzed and split into
   * map and reduce phases for more efficient execution across shards.
   *
   * - For pipelines with $group, partial aggregation runs on each shard
   *   and results are merged
   * - $sort + $limit optimization is applied when possible
   *
   * @default false
   */
  distributed?: boolean;
  /**
   * Batch size for streaming/batched document processing.
   * When set, documents are processed in batches instead of loading all at once.
   * This reduces memory usage for large collections.
   *
   * @default undefined (load all documents at once for backwards compatibility)
   */
  batchSize?: number;
}

/** Insert options */
export interface InsertOptions extends SessionOption {
  // Future: ordered, writeConcern, etc.
}

// ============================================================================
// Index Types
// ============================================================================

/** Index specification */
export interface IndexSpec {
  [key: string]: 1 | -1 | 'text' | '2d' | '2dsphere' | 'hashed';
}

/** Index options */
export interface IndexOptions {
  name?: string;
  unique?: boolean;
  sparse?: boolean;
  background?: boolean;
  expireAfterSeconds?: number;
  partialFilterExpression?: Filter<Document>;
  weights?: { [key: string]: number };
  default_language?: string;
  language_override?: string;
}

// ============================================================================
// Schema Types (MongoLake-specific)
// ============================================================================

/** Parquet column type */
export type ParquetType =
  | 'string'
  | 'int32'
  | 'int64'
  | 'float'
  | 'double'
  | 'boolean'
  | 'timestamp'
  | 'date'
  | 'binary'
  | 'variant';

/** Column definition */
export type ColumnDef =
  | ParquetType
  | [ParquetType]  // Array
  | { [key: string]: ColumnDef };  // Struct

/** Collection schema configuration */
export interface CollectionSchema {
  /** Promote these fields to native Parquet columns */
  columns?: { [key: string]: ColumnDef };

  /** Auto-promote fields appearing in >threshold of docs */
  autoPromote?: { threshold: number };

  /** Keep full document as variant (default: true) */
  storeVariant?: boolean;
}

/** Database schema configuration */
export interface SchemaConfig {
  [collection: string]: CollectionSchema;
}

// ============================================================================
// Storage Types (MongoLake-specific)
// ============================================================================

/** Internal row format in Parquet */
export interface ParquetRow {
  _id: string;
  _seq: number;
  _op: 'i' | 'u' | 'd';
  _data: Uint8Array;  // Variant-encoded full document
  [column: string]: unknown;  // Promoted columns
}

/** File metadata */
export interface FileMetadata {
  path: string;
  size: number;
  rowCount: number;
  minSeq: number;
  maxSeq: number;
  minId: string;
  maxId: string;
  columns: string[];
}

/** Collection manifest */
export interface CollectionManifest {
  name: string;
  files: FileMetadata[];
  schema: CollectionSchema;
  currentSeq: number;
  createdAt: string;
  updatedAt: string;
}

// ============================================================================
// Config Types
// ============================================================================

/** Connection string parsed information */
export interface ConnectionStringInfo {
  /** List of hosts from the connection string */
  hosts: Array<{ host: string; port: number }>;
  /** Username for authentication */
  username?: string;
  /** Password for authentication */
  password?: string;
  /** Connection options */
  options: ConnectionOptions;
}

/** MongoLake configuration */
export interface MongoLakeConfig {
  /** Local filesystem storage */
  local?: string;

  /** Cloudflare R2 bucket */
  bucket?: R2Bucket;

  /** S3-compatible storage */
  endpoint?: string;
  accessKeyId?: string;
  secretAccessKey?: string;
  bucketName?: string;

  /** Database name */
  database?: string;

  /** Iceberg integration */
  iceberg?: boolean | {
    token: string;
    catalog?: string;
  };

  /** Schema configuration */
  schema?: SchemaConfig;

  /** Branch to use */
  branch?: string;

  /** Query as of timestamp */
  asOf?: string | Date | number;

  /**
   * Parsed connection string information
   * Populated when the client is created with a MongoDB connection string
   * @internal
   */
  connectionString?: ConnectionStringInfo;
}

/** R2Bucket type (from Cloudflare Workers) */
export interface R2Bucket {
  get(key: string): Promise<R2ObjectBody | null>;
  head(key: string): Promise<R2Object | null>;
  put(key: string, value: ReadableStream | ArrayBuffer | Uint8Array | string | Blob | null): Promise<R2Object>;
  delete(key: string): Promise<void>;
  list(options?: R2ListOptions): Promise<R2Objects>;
  createMultipartUpload(key: string): Promise<R2MultipartUpload>;
}

export interface R2ObjectBody {
  arrayBuffer(): Promise<ArrayBuffer>;
  text(): Promise<string>;
  json<T>(): Promise<T>;
  body: ReadableStream;
  etag: string;
}

export interface R2Object {
  key: string;
  size: number;
  etag: string;
}

export interface R2ListOptions {
  prefix?: string;
  limit?: number;
  cursor?: string;
}

export interface R2Objects {
  objects: R2Object[];
  truncated: boolean;
  cursor?: string;
}

export interface R2MultipartUpload {
  uploadPart(partNumber: number, value: ArrayBuffer | Uint8Array): Promise<R2UploadedPart>;
  complete(parts: R2UploadedPart[]): Promise<R2Object>;
  abort(): Promise<void>;
}

export interface R2UploadedPart {
  partNumber: number;
  etag: string;
}

// ============================================================================
// Service Binding Types (Cloudflare Workers)
// ============================================================================

/**
 * AUTH service binding for token validation
 *
 * Service binding provides in-datacenter communication with minimal latency.
 * The fetch method sends requests directly to the bound auth-service worker.
 */
export interface AuthServiceBinding {
  /**
   * Fetch from the AUTH service
   *
   * Supports endpoints:
   * - POST /validate - Validate a token
   * - POST /introspect - Get detailed token info
   */
  fetch(request: Request): Promise<Response>;
}

/**
 * OAUTH service binding for token operations
 *
 * Service binding provides in-datacenter communication with minimal latency.
 * The fetch method sends requests directly to the bound oauth-service worker.
 */
export interface OAuthServiceBinding {
  /**
   * Fetch from the OAUTH service
   *
   * Supports endpoints:
   * - POST /token - Token exchange/refresh
   * - POST /revoke - Revoke tokens
   * - GET /userinfo - Get user information
   */
  fetch(request: Request): Promise<Response>;
}

/**
 * Environment bindings with optional AUTH and OAUTH services
 *
 * These service bindings are optional and enable low-latency
 * authentication when configured in wrangler.toml.
 */
export interface ServiceBindingEnv {
  /** AUTH service binding for token validation */
  AUTH?: AuthServiceBinding;
  /** OAUTH service binding for token refresh/exchange */
  OAUTH?: OAuthServiceBinding;
}
