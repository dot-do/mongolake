/**
 * MongoLake Types
 *
 * MongoDB-compatible type definitions
 */

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
    // Random (5 bytes)
    crypto.getRandomValues(bytes.subarray(4, 9));
    // Counter (3 bytes)
    const counter = Math.floor(Math.random() * 0xffffff);
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
      (this.bytes[0] << 24) |
      (this.bytes[1] << 16) |
      (this.bytes[2] << 8) |
      this.bytes[3];
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

/** Document with required _id */
export interface WithId<T extends Document> extends T {
  _id: string | ObjectId;
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

/** Array operators */
export interface ArrayOperators<T> {
  $all?: T[];
  $elemMatch?: Filter<T extends (infer U)[] ? U : never>;
  $size?: number;
}

/** Filter condition for a field */
export type FilterCondition<T> =
  | T
  | ComparisonOperators<T>
  | ElementOperators
  | ArrayOperators<T>;

/** Logical operators */
export interface LogicalOperators<T> {
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
  | { $group: { _id: unknown; [key: string]: AccumulatorExpression } }
  | { $sort: { [key: string]: 1 | -1 } }
  | { $limit: number }
  | { $skip: number }
  | { $project: { [key: string]: 0 | 1 | ProjectionExpression } }
  | { $unwind: string | { path: string; preserveNullAndEmptyArrays?: boolean } }
  | { $lookup: LookupStage }
  | { $count: string }
  | { $addFields: { [key: string]: unknown } }
  | { $set: { [key: string]: unknown } }
  | { $unset: string | string[] };

/** Accumulator expressions for $group */
export interface AccumulatorExpression {
  $sum?: number | string;
  $avg?: string;
  $min?: string;
  $max?: string;
  $first?: string;
  $last?: string;
  $push?: string;
  $addToSet?: string;
  $count?: Record<string, never>;
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
// Options Types
// ============================================================================

/** Find options */
export interface FindOptions {
  projection?: { [key: string]: 0 | 1 };
  sort?: { [key: string]: 1 | -1 };
  limit?: number;
  skip?: number;
  hint?: string | { [key: string]: 1 | -1 };
  maxTimeMS?: number;
}

/** Update options */
export interface UpdateOptions {
  upsert?: boolean;
  arrayFilters?: Filter<Document>[];
  hint?: string | { [key: string]: 1 | -1 };
}

/** Delete options */
export interface DeleteOptions {
  hint?: string | { [key: string]: 1 | -1 };
}

/** Aggregate options */
export interface AggregateOptions {
  allowDiskUse?: boolean;
  maxTimeMS?: number;
  hint?: string | { [key: string]: 1 | -1 };
}

// ============================================================================
// Index Types
// ============================================================================

/** Index specification */
export interface IndexSpec {
  [key: string]: 1 | -1 | 'text' | '2dsphere' | 'hashed';
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
}

/** R2Bucket type (from Cloudflare Workers) */
export interface R2Bucket {
  get(key: string): Promise<R2ObjectBody | null>;
  put(key: string, value: ArrayBuffer | Uint8Array | string): Promise<R2Object>;
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
