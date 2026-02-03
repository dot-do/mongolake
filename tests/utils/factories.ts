/**
 * Test Factories
 *
 * Factory functions for generating test documents, ObjectIds, dates,
 * and nested documents. Reduces boilerplate in tests.
 */

import { ObjectId, type Document, type AnyDocument } from '../../src/types.js';

// ============================================================================
// ObjectId Factory
// ============================================================================

/**
 * Generate a new ObjectId.
 */
export function createObjectId(): ObjectId {
  return new ObjectId();
}

/**
 * Generate a new ObjectId as a hex string.
 */
export function createObjectIdString(): string {
  return new ObjectId().toString();
}

/**
 * Generate an ObjectId from a specific timestamp.
 * @param date - The date to use for the timestamp portion
 */
export function createObjectIdFromDate(date: Date): ObjectId {
  const timestamp = Math.floor(date.getTime() / 1000);
  const timestampHex = timestamp.toString(16).padStart(8, '0');
  // Use zeros for the random and counter portions
  const hex = timestampHex + '000000000000000000';
  return new ObjectId(hex.slice(0, 24));
}

/**
 * Generate multiple unique ObjectIds.
 * @param count - Number of ObjectIds to generate
 */
export function createObjectIds(count: number): ObjectId[] {
  return Array.from({ length: count }, () => new ObjectId());
}

// ============================================================================
// Date Factory
// ============================================================================

/**
 * Create a date relative to now.
 * @param offsetMs - Offset in milliseconds (negative for past, positive for future)
 */
export function createDate(offsetMs: number = 0): Date {
  return new Date(Date.now() + offsetMs);
}

/**
 * Create a date in the past.
 * @param daysAgo - Number of days ago
 */
export function createPastDate(daysAgo: number): Date {
  return createDate(-daysAgo * 24 * 60 * 60 * 1000);
}

/**
 * Create a date in the future.
 * @param daysFromNow - Number of days from now
 */
export function createFutureDate(daysFromNow: number): Date {
  return createDate(daysFromNow * 24 * 60 * 60 * 1000);
}

/**
 * Create a date at a specific time.
 * @param year - Year
 * @param month - Month (1-12)
 * @param day - Day of month
 * @param hour - Hour (0-23)
 * @param minute - Minute (0-59)
 * @param second - Second (0-59)
 */
export function createDateAt(
  year: number,
  month: number,
  day: number,
  hour: number = 0,
  minute: number = 0,
  second: number = 0
): Date {
  return new Date(year, month - 1, day, hour, minute, second);
}

// ============================================================================
// Document Factory Types
// ============================================================================

export interface UserDocument extends Document {
  _id: string | ObjectId;
  name: string;
  email?: string;
  age?: number;
  status?: 'active' | 'inactive' | 'pending';
  tags?: string[];
  profile?: {
    firstName?: string;
    lastName?: string;
    bio?: string;
    avatar?: string;
  };
  settings?: {
    theme?: 'light' | 'dark';
    notifications?: boolean;
    language?: string;
  };
  address?: AddressDocument;
  createdAt?: Date;
  updatedAt?: Date;
}

export interface AddressDocument {
  street?: string;
  city?: string;
  state?: string;
  country?: string;
  zip?: string;
  coordinates?: {
    lat: number;
    lng: number;
  };
}

export interface OrderDocument extends Document {
  _id: string | ObjectId;
  userId: string | ObjectId;
  items: OrderItem[];
  status: 'pending' | 'processing' | 'shipped' | 'delivered' | 'cancelled';
  total: number;
  shippingAddress?: AddressDocument;
  createdAt?: Date;
  updatedAt?: Date;
}

export interface OrderItem {
  productId: string | ObjectId;
  name: string;
  quantity: number;
  price: number;
}

export interface ProductDocument extends Document {
  _id: string | ObjectId;
  name: string;
  description?: string;
  price: number;
  category?: string;
  tags?: string[];
  inventory?: number;
  metadata?: Record<string, unknown>;
  createdAt?: Date;
  updatedAt?: Date;
}

export interface DeduplicationDocument extends Document {
  _id: string;
  _seq: number;
  _op: 'i' | 'u' | 'd';
  [key: string]: unknown;
}

// ============================================================================
// Document Factories
// ============================================================================

let docCounter = 0;

/**
 * Reset the document counter (useful between tests).
 */
export function resetDocumentCounter(): void {
  docCounter = 0;
}

/**
 * Create a user document with sensible defaults.
 * @param overrides - Fields to override
 */
export function createUser(overrides: Partial<UserDocument> = {}): UserDocument {
  docCounter++;
  const now = new Date();
  return {
    _id: overrides._id ?? `user-${docCounter}`,
    name: overrides.name ?? `Test User ${docCounter}`,
    email: overrides.email ?? `user${docCounter}@example.com`,
    age: overrides.age ?? 25 + (docCounter % 50),
    status: overrides.status ?? 'active',
    tags: overrides.tags ?? ['user'],
    createdAt: overrides.createdAt ?? now,
    updatedAt: overrides.updatedAt ?? now,
    ...overrides,
  };
}

/**
 * Create multiple user documents.
 * @param count - Number of users to create
 * @param overrides - Fields to override for all users
 */
export function createUsers(count: number, overrides: Partial<UserDocument> = {}): UserDocument[] {
  return Array.from({ length: count }, () => createUser(overrides));
}

/**
 * Create an order document with sensible defaults.
 * @param overrides - Fields to override
 */
export function createOrder(overrides: Partial<OrderDocument> = {}): OrderDocument {
  docCounter++;
  const now = new Date();
  const items = overrides.items ?? [
    createOrderItem({ productId: `product-${docCounter}` }),
  ];
  const total = overrides.total ?? items.reduce((sum, item) => sum + item.price * item.quantity, 0);

  return {
    _id: overrides._id ?? `order-${docCounter}`,
    userId: overrides.userId ?? `user-${docCounter}`,
    items,
    status: overrides.status ?? 'pending',
    total,
    createdAt: overrides.createdAt ?? now,
    updatedAt: overrides.updatedAt ?? now,
    ...overrides,
  };
}

/**
 * Create an order item.
 * @param overrides - Fields to override
 */
export function createOrderItem(overrides: Partial<OrderItem> = {}): OrderItem {
  docCounter++;
  return {
    productId: overrides.productId ?? `product-${docCounter}`,
    name: overrides.name ?? `Product ${docCounter}`,
    quantity: overrides.quantity ?? 1,
    price: overrides.price ?? 9.99,
    ...overrides,
  };
}

/**
 * Create a product document with sensible defaults.
 * @param overrides - Fields to override
 */
export function createProduct(overrides: Partial<ProductDocument> = {}): ProductDocument {
  docCounter++;
  const now = new Date();
  return {
    _id: overrides._id ?? `product-${docCounter}`,
    name: overrides.name ?? `Test Product ${docCounter}`,
    description: overrides.description ?? `Description for product ${docCounter}`,
    price: overrides.price ?? 19.99 + docCounter,
    category: overrides.category ?? 'general',
    tags: overrides.tags ?? ['product'],
    inventory: overrides.inventory ?? 100,
    createdAt: overrides.createdAt ?? now,
    updatedAt: overrides.updatedAt ?? now,
    ...overrides,
  };
}

/**
 * Create a deduplication document (for deduplication engine tests).
 * @param id - Document ID
 * @param seq - Sequence number
 * @param op - Operation type
 * @param data - Additional data fields
 */
export function createDeduplicationDoc(
  id: string,
  seq: number,
  op: 'i' | 'u' | 'd' = 'i',
  data: Record<string, unknown> = {}
): DeduplicationDocument {
  return {
    _id: id,
    _seq: seq,
    _op: op,
    ...data,
  };
}

/**
 * Create multiple deduplication documents for the same ID.
 * @param id - Document ID
 * @param seqs - Array of sequence numbers
 * @param finalOp - Operation for the final document
 */
export function createDeduplicationSequence(
  id: string,
  seqs: number[],
  finalOp: 'i' | 'u' | 'd' = 'u'
): DeduplicationDocument[] {
  return seqs.map((seq, index) => {
    const isLast = index === seqs.length - 1;
    const op = index === 0 ? 'i' : isLast ? finalOp : 'u';
    return createDeduplicationDoc(id, seq, op, { name: `v${seq}` });
  });
}

// ============================================================================
// Address Factory
// ============================================================================

/**
 * Create an address document.
 * @param overrides - Fields to override
 */
export function createAddress(overrides: Partial<AddressDocument> = {}): AddressDocument {
  docCounter++;
  return {
    street: overrides.street ?? `${docCounter} Main St`,
    city: overrides.city ?? 'New York',
    state: overrides.state ?? 'NY',
    country: overrides.country ?? 'USA',
    zip: overrides.zip ?? `1000${docCounter % 10}`,
    ...overrides,
  };
}

/**
 * Create an address with coordinates.
 * @param overrides - Fields to override
 */
export function createAddressWithCoordinates(
  overrides: Partial<AddressDocument> = {}
): AddressDocument {
  return {
    ...createAddress(overrides),
    coordinates: overrides.coordinates ?? {
      lat: 40.7128 + (Math.random() - 0.5) * 0.1,
      lng: -74.006 + (Math.random() - 0.5) * 0.1,
    },
  };
}

// ============================================================================
// Nested Document Factory
// ============================================================================

/**
 * Create a deeply nested document for testing nested field access.
 * @param depth - How many levels deep to nest
 * @param value - The value at the deepest level
 */
export function createNestedDocument(
  depth: number,
  value: unknown = 'leaf'
): Record<string, unknown> {
  if (depth <= 0) {
    return { value };
  }
  return { nested: createNestedDocument(depth - 1, value) };
}

/**
 * Create a document with a specific nested path and value.
 * @param path - Dot-separated path (e.g., 'a.b.c')
 * @param value - The value to set at the path
 */
export function createDocumentWithPath(
  path: string,
  value: unknown
): Record<string, unknown> {
  const parts = path.split('.');
  let result: Record<string, unknown> = {};
  let current = result;

  for (let i = 0; i < parts.length - 1; i++) {
    current[parts[i]] = {};
    current = current[parts[i]] as Record<string, unknown>;
  }

  current[parts[parts.length - 1]] = value;
  return result;
}

// ============================================================================
// Bulk Document Factory
// ============================================================================

/**
 * Create a batch of documents with varying data.
 * @param count - Number of documents to create
 * @param generator - Function to generate each document
 */
export function createBatch<T>(count: number, generator: (index: number) => T): T[] {
  return Array.from({ length: count }, (_, i) => generator(i));
}

/**
 * Create a document with many keys.
 * @param keyCount - Number of keys to add
 * @param idPrefix - Prefix for the document ID
 */
export function createDocumentWithManyKeys(
  keyCount: number,
  idPrefix: string = 'doc'
): Record<string, unknown> {
  const doc: Record<string, unknown> = { _id: `${idPrefix}-${++docCounter}` };
  for (let i = 0; i < keyCount; i++) {
    doc[`key${i}`] = `value${i}`;
  }
  return doc;
}

/**
 * Create a document with a large string field.
 * @param size - Size of the string in characters
 * @param idPrefix - Prefix for the document ID
 */
export function createLargeDocument(
  size: number,
  idPrefix: string = 'large'
): Record<string, unknown> {
  return {
    _id: `${idPrefix}-${++docCounter}`,
    data: 'x'.repeat(size),
    description: 'Large document for testing',
  };
}

// ============================================================================
// Type Helpers
// ============================================================================

/**
 * Create a generic document with specific fields.
 * @param fields - Fields to include in the document
 */
export function createDocument<T extends Document>(fields: T): T {
  return fields;
}

/**
 * Create a document that matches a filter (for testing filter matching).
 * @param filter - The filter to match
 */
export function createMatchingDocument(filter: Record<string, unknown>): Record<string, unknown> {
  const doc: Record<string, unknown> = { _id: `match-${++docCounter}` };

  for (const [key, value] of Object.entries(filter)) {
    // Skip logical operators
    if (key.startsWith('$')) continue;

    // Handle comparison operators
    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      const ops = value as Record<string, unknown>;
      if ('$eq' in ops) {
        doc[key] = ops.$eq;
      } else if ('$gt' in ops) {
        doc[key] = (ops.$gt as number) + 1;
      } else if ('$gte' in ops) {
        doc[key] = ops.$gte;
      } else if ('$lt' in ops) {
        doc[key] = (ops.$lt as number) - 1;
      } else if ('$lte' in ops) {
        doc[key] = ops.$lte;
      } else if ('$in' in ops) {
        doc[key] = (ops.$in as unknown[])[0];
      } else {
        doc[key] = value;
      }
    } else {
      doc[key] = value;
    }
  }

  return doc;
}

// ============================================================================
// Parquet File Factory
// ============================================================================

/**
 * Parquet file metadata for testing.
 */
export interface ParquetFileMetadata {
  path: string;
  size: number;
  rowCount: number;
  minLSN: number;
  maxLSN: number;
  createdAt: number;
  zoneMap?: ZoneMapEntry[];
}

/**
 * Zone map entry for predicate pushdown testing.
 */
export interface ZoneMapEntry {
  field: string;
  min: string | number | boolean | null;
  max: string | number | boolean | null;
  nullCount: number;
  rowCount: number;
}

/**
 * Builder class for creating Parquet file metadata.
 */
export class ParquetFileBuilder {
  private metadata: ParquetFileMetadata;

  constructor() {
    docCounter++;
    const now = Date.now();
    this.metadata = {
      path: `data/collection-${docCounter}/${now}/file-${docCounter}.parquet`,
      size: 1024 * (docCounter + 1),
      rowCount: 100 * (docCounter + 1),
      minLSN: docCounter * 100,
      maxLSN: docCounter * 100 + 99,
      createdAt: now,
    };
  }

  withPath(path: string): this {
    this.metadata.path = path;
    return this;
  }

  withCollection(collection: string): this {
    const date = new Date().toISOString().split('T')[0];
    this.metadata.path = `data/${collection}/${date}/file-${docCounter}.parquet`;
    return this;
  }

  withSize(size: number): this {
    this.metadata.size = size;
    return this;
  }

  withRowCount(count: number): this {
    this.metadata.rowCount = count;
    return this;
  }

  withLSNRange(min: number, max: number): this {
    this.metadata.minLSN = min;
    this.metadata.maxLSN = max;
    return this;
  }

  withCreatedAt(timestamp: number | Date): this {
    this.metadata.createdAt = timestamp instanceof Date ? timestamp.getTime() : timestamp;
    return this;
  }

  withZoneMap(entries: ZoneMapEntry[]): this {
    this.metadata.zoneMap = entries;
    return this;
  }

  addZoneMapEntry(entry: Partial<ZoneMapEntry> & { field: string }): this {
    if (!this.metadata.zoneMap) {
      this.metadata.zoneMap = [];
    }
    this.metadata.zoneMap.push({
      field: entry.field,
      min: entry.min ?? 0,
      max: entry.max ?? 100,
      nullCount: entry.nullCount ?? 0,
      rowCount: entry.rowCount ?? this.metadata.rowCount,
    });
    return this;
  }

  build(): ParquetFileMetadata {
    return { ...this.metadata };
  }
}

/**
 * Create a Parquet file metadata object.
 */
export function createParquetFile(overrides: Partial<ParquetFileMetadata> = {}): ParquetFileMetadata {
  const builder = new ParquetFileBuilder();
  if (overrides.path) builder.withPath(overrides.path);
  if (overrides.size) builder.withSize(overrides.size);
  if (overrides.rowCount) builder.withRowCount(overrides.rowCount);
  if (overrides.minLSN !== undefined && overrides.maxLSN !== undefined) {
    builder.withLSNRange(overrides.minLSN, overrides.maxLSN);
  }
  if (overrides.createdAt) builder.withCreatedAt(overrides.createdAt);
  if (overrides.zoneMap) builder.withZoneMap(overrides.zoneMap);
  return builder.build();
}

/**
 * Create multiple Parquet files with sequential LSN ranges.
 */
export function createParquetFiles(count: number, options: {
  collection?: string;
  startLSN?: number;
  docsPerFile?: number;
} = {}): ParquetFileMetadata[] {
  const { collection = 'test', startLSN = 0, docsPerFile = 100 } = options;
  const files: ParquetFileMetadata[] = [];

  for (let i = 0; i < count; i++) {
    const minLSN = startLSN + i * docsPerFile;
    const maxLSN = minLSN + docsPerFile - 1;
    files.push(
      new ParquetFileBuilder()
        .withCollection(collection)
        .withLSNRange(minLSN, maxLSN)
        .withRowCount(docsPerFile)
        .build()
    );
  }

  return files;
}

// ============================================================================
// WAL Entry Factory
// ============================================================================

/**
 * Write-Ahead Log entry for durability testing.
 */
export interface WalEntry {
  lsn: number;
  collection: string;
  op: 'i' | 'u' | 'd';
  docId: string;
  document: Record<string, unknown>;
  flushed: boolean;
}

/**
 * Builder class for creating WAL entries.
 */
export class WalEntryBuilder {
  private entry: WalEntry;

  constructor() {
    docCounter++;
    this.entry = {
      lsn: docCounter,
      collection: 'test',
      op: 'i',
      docId: `doc-${docCounter}`,
      document: { _id: `doc-${docCounter}`, name: `Test Doc ${docCounter}` },
      flushed: false,
    };
  }

  withLSN(lsn: number): this {
    this.entry.lsn = lsn;
    return this;
  }

  withCollection(collection: string): this {
    this.entry.collection = collection;
    return this;
  }

  withOperation(op: 'i' | 'u' | 'd'): this {
    this.entry.op = op;
    return this;
  }

  insert(): this {
    this.entry.op = 'i';
    return this;
  }

  update(): this {
    this.entry.op = 'u';
    return this;
  }

  delete(): this {
    this.entry.op = 'd';
    return this;
  }

  withDocId(docId: string): this {
    this.entry.docId = docId;
    this.entry.document._id = docId;
    return this;
  }

  withDocument(document: Record<string, unknown>): this {
    this.entry.document = document;
    if (document._id) {
      this.entry.docId = String(document._id);
    }
    return this;
  }

  flushed(value: boolean = true): this {
    this.entry.flushed = value;
    return this;
  }

  build(): WalEntry {
    return { ...this.entry, document: { ...this.entry.document } };
  }
}

/**
 * Create a WAL entry.
 */
export function createWalEntry(overrides: Partial<WalEntry> = {}): WalEntry {
  const builder = new WalEntryBuilder();
  if (overrides.lsn !== undefined) builder.withLSN(overrides.lsn);
  if (overrides.collection) builder.withCollection(overrides.collection);
  if (overrides.op) builder.withOperation(overrides.op);
  if (overrides.docId) builder.withDocId(overrides.docId);
  if (overrides.document) builder.withDocument(overrides.document);
  if (overrides.flushed !== undefined) builder.flushed(overrides.flushed);
  return builder.build();
}

/**
 * Create multiple WAL entries for a sequence of operations.
 */
export function createWalSequence(operations: Array<{
  op: 'i' | 'u' | 'd';
  docId?: string;
  document?: Record<string, unknown>;
}>, options: { collection?: string; startLSN?: number } = {}): WalEntry[] {
  const { collection = 'test', startLSN = 1 } = options;

  return operations.map((operation, index) => {
    const builder = new WalEntryBuilder()
      .withLSN(startLSN + index)
      .withCollection(collection)
      .withOperation(operation.op);

    if (operation.docId) builder.withDocId(operation.docId);
    if (operation.document) builder.withDocument(operation.document);

    return builder.build();
  });
}

// ============================================================================
// Shard State Factory
// ============================================================================

/**
 * Shard state for router and DO testing.
 */
export interface ShardState {
  shardId: string;
  currentLSN: number;
  collections: string[];
  fileCount: number;
  totalSize: number;
  lastFlush: number;
  isHealthy: boolean;
}

/**
 * Builder class for creating shard state.
 */
export class ShardStateBuilder {
  private state: ShardState;

  constructor() {
    docCounter++;
    this.state = {
      shardId: `shard-${docCounter % 16}`,
      currentLSN: docCounter * 100,
      collections: ['test'],
      fileCount: 5,
      totalSize: 1024 * 1024,
      lastFlush: Date.now(),
      isHealthy: true,
    };
  }

  withShardId(shardId: string | number): this {
    this.state.shardId = typeof shardId === 'number' ? `shard-${shardId}` : shardId;
    return this;
  }

  withCurrentLSN(lsn: number): this {
    this.state.currentLSN = lsn;
    return this;
  }

  withCollections(collections: string[]): this {
    this.state.collections = [...collections];
    return this;
  }

  addCollection(collection: string): this {
    if (!this.state.collections.includes(collection)) {
      this.state.collections.push(collection);
    }
    return this;
  }

  withFileCount(count: number): this {
    this.state.fileCount = count;
    return this;
  }

  withTotalSize(size: number): this {
    this.state.totalSize = size;
    return this;
  }

  withLastFlush(timestamp: number | Date): this {
    this.state.lastFlush = timestamp instanceof Date ? timestamp.getTime() : timestamp;
    return this;
  }

  healthy(value: boolean = true): this {
    this.state.isHealthy = value;
    return this;
  }

  unhealthy(): this {
    this.state.isHealthy = false;
    return this;
  }

  build(): ShardState {
    return { ...this.state, collections: [...this.state.collections] };
  }
}

/**
 * Create a shard state object.
 */
export function createShardState(overrides: Partial<ShardState> = {}): ShardState {
  const builder = new ShardStateBuilder();
  if (overrides.shardId) builder.withShardId(overrides.shardId);
  if (overrides.currentLSN !== undefined) builder.withCurrentLSN(overrides.currentLSN);
  if (overrides.collections) builder.withCollections(overrides.collections);
  if (overrides.fileCount !== undefined) builder.withFileCount(overrides.fileCount);
  if (overrides.totalSize !== undefined) builder.withTotalSize(overrides.totalSize);
  if (overrides.lastFlush !== undefined) builder.withLastFlush(overrides.lastFlush);
  if (overrides.isHealthy !== undefined) builder.healthy(overrides.isHealthy);
  return builder.build();
}

/**
 * Create multiple shard states for a cluster.
 */
export function createShardCluster(shardCount: number, options: {
  collections?: string[];
  healthy?: boolean;
} = {}): ShardState[] {
  const { collections = ['test'], healthy = true } = options;
  const states: ShardState[] = [];

  for (let i = 0; i < shardCount; i++) {
    states.push(
      new ShardStateBuilder()
        .withShardId(i)
        .withCollections(collections)
        .healthy(healthy)
        .build()
    );
  }

  return states;
}

// ============================================================================
// Branch Metadata Factory
// ============================================================================

/**
 * Branch state for branching feature testing.
 */
export type BranchState = 'active' | 'merged' | 'deleted';

/**
 * Branch metadata for database branching testing.
 */
export interface BranchMetadata {
  version?: number;
  name: string;
  baseCommit: string;
  headCommit: string;
  createdAt: string;
  updatedAt: string;
  description?: string;
  createdBy?: string;
  protected?: boolean;
  metadata?: Record<string, unknown>;
  state: BranchState;
  parentBranch: string | null;
  branchSequence: number;
  modifiedFiles?: string[];
  mergeCommit?: string;
  mergedInto?: string;
}

/**
 * Builder class for creating branch metadata.
 */
export class BranchMetadataBuilder {
  private branch: BranchMetadata;

  constructor(name?: string) {
    docCounter++;
    const now = new Date().toISOString();
    const branchName = name ?? `branch-${docCounter}`;
    this.branch = {
      version: 1,
      name: branchName,
      baseCommit: `commit-${docCounter}`,
      headCommit: `commit-${docCounter}`,
      createdAt: now,
      updatedAt: now,
      state: 'active',
      parentBranch: 'main',
      branchSequence: docCounter,
      modifiedFiles: [],
    };
  }

  withName(name: string): this {
    this.branch.name = name;
    return this;
  }

  withBaseCommit(commit: string): this {
    this.branch.baseCommit = commit;
    return this;
  }

  withHeadCommit(commit: string): this {
    this.branch.headCommit = commit;
    return this;
  }

  withCommits(baseCommit: string, headCommit?: string): this {
    this.branch.baseCommit = baseCommit;
    this.branch.headCommit = headCommit ?? baseCommit;
    return this;
  }

  withDescription(description: string): this {
    this.branch.description = description;
    return this;
  }

  withCreatedBy(createdBy: string): this {
    this.branch.createdBy = createdBy;
    return this;
  }

  protected(value: boolean = true): this {
    this.branch.protected = value;
    return this;
  }

  withMetadata(metadata: Record<string, unknown>): this {
    this.branch.metadata = { ...metadata };
    return this;
  }

  withState(state: BranchState): this {
    this.branch.state = state;
    return this;
  }

  active(): this {
    this.branch.state = 'active';
    return this;
  }

  merged(intoTarget?: string, mergeCommit?: string): this {
    this.branch.state = 'merged';
    if (intoTarget) this.branch.mergedInto = intoTarget;
    if (mergeCommit) this.branch.mergeCommit = mergeCommit;
    return this;
  }

  deleted(): this {
    this.branch.state = 'deleted';
    return this;
  }

  withParentBranch(parent: string | null): this {
    this.branch.parentBranch = parent;
    return this;
  }

  withBranchSequence(sequence: number): this {
    this.branch.branchSequence = sequence;
    return this;
  }

  withModifiedFiles(files: string[]): this {
    this.branch.modifiedFiles = [...files];
    return this;
  }

  addModifiedFile(file: string): this {
    if (!this.branch.modifiedFiles) {
      this.branch.modifiedFiles = [];
    }
    if (!this.branch.modifiedFiles.includes(file)) {
      this.branch.modifiedFiles.push(file);
    }
    return this;
  }

  withCreatedAt(date: string | Date): this {
    this.branch.createdAt = date instanceof Date ? date.toISOString() : date;
    return this;
  }

  withUpdatedAt(date: string | Date): this {
    this.branch.updatedAt = date instanceof Date ? date.toISOString() : date;
    return this;
  }

  asMainBranch(): this {
    this.branch.name = 'main';
    this.branch.parentBranch = null;
    this.branch.protected = true;
    this.branch.branchSequence = 0;
    return this;
  }

  build(): BranchMetadata {
    return {
      ...this.branch,
      modifiedFiles: this.branch.modifiedFiles ? [...this.branch.modifiedFiles] : [],
      metadata: this.branch.metadata ? { ...this.branch.metadata } : undefined,
    };
  }
}

/**
 * Create branch metadata.
 */
export function createBranchMetadata(overrides: Partial<BranchMetadata> = {}): BranchMetadata {
  const builder = new BranchMetadataBuilder(overrides.name);
  if (overrides.baseCommit) builder.withBaseCommit(overrides.baseCommit);
  if (overrides.headCommit) builder.withHeadCommit(overrides.headCommit);
  if (overrides.description) builder.withDescription(overrides.description);
  if (overrides.createdBy) builder.withCreatedBy(overrides.createdBy);
  if (overrides.protected !== undefined) builder.protected(overrides.protected);
  if (overrides.metadata) builder.withMetadata(overrides.metadata);
  if (overrides.state) builder.withState(overrides.state);
  if (overrides.parentBranch !== undefined) builder.withParentBranch(overrides.parentBranch);
  if (overrides.branchSequence !== undefined) builder.withBranchSequence(overrides.branchSequence);
  if (overrides.modifiedFiles) builder.withModifiedFiles(overrides.modifiedFiles);
  if (overrides.createdAt) builder.withCreatedAt(overrides.createdAt);
  if (overrides.updatedAt) builder.withUpdatedAt(overrides.updatedAt);
  return builder.build();
}

/**
 * Create the main branch metadata.
 */
export function createMainBranch(initialCommit: string = 'initial'): BranchMetadata {
  return new BranchMetadataBuilder()
    .asMainBranch()
    .withCommits(initialCommit)
    .build();
}

/**
 * Create a branch hierarchy for testing.
 */
export function createBranchHierarchy(depth: number, options: {
  baseCommit?: string;
} = {}): BranchMetadata[] {
  const { baseCommit = 'initial' } = options;
  const branches: BranchMetadata[] = [];

  // Create main branch
  branches.push(createMainBranch(baseCommit));

  // Create child branches
  let parentBranch = 'main';
  let currentCommit = baseCommit;

  for (let i = 1; i <= depth; i++) {
    const newCommit = `commit-${i}`;
    const branch = new BranchMetadataBuilder(`feature-level-${i}`)
      .withParentBranch(parentBranch)
      .withCommits(currentCommit, newCommit)
      .withBranchSequence(i)
      .build();
    branches.push(branch);
    parentBranch = branch.name;
    currentCommit = newCommit;
  }

  return branches;
}

// ============================================================================
// Transaction State Factory
// ============================================================================

/**
 * Transaction state for testing.
 */
export type TransactionState = 'none' | 'starting' | 'in_progress' | 'committed' | 'aborted';

/**
 * Read concern levels for transactions.
 */
export type ReadConcernLevel = 'local' | 'majority' | 'linearizable' | 'snapshot';

/**
 * Transaction options.
 */
export interface TransactionOptions {
  readConcern?: { level: ReadConcernLevel };
  writeConcern?: { w?: number | 'majority'; j?: boolean; wtimeout?: number };
  maxCommitTimeMS?: number;
}

/**
 * Buffered operation during a transaction.
 */
export interface BufferedOperation {
  type: 'insert' | 'update' | 'delete' | 'replace';
  collection: string;
  database: string;
  document?: Record<string, unknown>;
  filter?: Record<string, unknown>;
  update?: Record<string, unknown>;
  replacement?: Record<string, unknown>;
  options?: Record<string, unknown>;
  timestamp: number;
}

/**
 * Transaction state for testing transaction management.
 */
export interface TransactionTestState {
  sessionId: string;
  txnNumber: number;
  state: TransactionState;
  options: TransactionOptions | null;
  operations: BufferedOperation[];
  startTime: number;
  commitTime?: number;
}

/**
 * Builder class for creating transaction test state.
 */
export class TransactionStateBuilder {
  private txnState: TransactionTestState;

  constructor() {
    docCounter++;
    this.txnState = {
      sessionId: crypto.randomUUID(),
      txnNumber: docCounter,
      state: 'none',
      options: null,
      operations: [],
      startTime: Date.now(),
    };
  }

  withSessionId(sessionId: string): this {
    this.txnState.sessionId = sessionId;
    return this;
  }

  withTxnNumber(txnNumber: number): this {
    this.txnState.txnNumber = txnNumber;
    return this;
  }

  withState(state: TransactionState): this {
    this.txnState.state = state;
    return this;
  }

  starting(): this {
    this.txnState.state = 'starting';
    return this;
  }

  inProgress(): this {
    this.txnState.state = 'in_progress';
    return this;
  }

  committed(): this {
    this.txnState.state = 'committed';
    this.txnState.commitTime = Date.now();
    return this;
  }

  aborted(): this {
    this.txnState.state = 'aborted';
    return this;
  }

  withOptions(options: TransactionOptions): this {
    this.txnState.options = { ...options };
    return this;
  }

  withReadConcern(level: ReadConcernLevel): this {
    if (!this.txnState.options) this.txnState.options = {};
    this.txnState.options.readConcern = { level };
    return this;
  }

  withWriteConcern(w: number | 'majority', j?: boolean): this {
    if (!this.txnState.options) this.txnState.options = {};
    this.txnState.options.writeConcern = { w, j };
    return this;
  }

  withOperations(operations: BufferedOperation[]): this {
    this.txnState.operations = [...operations];
    return this;
  }

  addOperation(operation: Partial<BufferedOperation> & { type: BufferedOperation['type']; collection: string }): this {
    this.txnState.operations.push({
      type: operation.type,
      collection: operation.collection,
      database: operation.database ?? 'test',
      document: operation.document,
      filter: operation.filter,
      update: operation.update,
      replacement: operation.replacement,
      options: operation.options,
      timestamp: operation.timestamp ?? Date.now(),
    });
    return this;
  }

  addInsert(collection: string, document: Record<string, unknown>, database: string = 'test'): this {
    return this.addOperation({
      type: 'insert',
      collection,
      database,
      document,
    });
  }

  addUpdate(collection: string, filter: Record<string, unknown>, update: Record<string, unknown>, database: string = 'test'): this {
    return this.addOperation({
      type: 'update',
      collection,
      database,
      filter,
      update,
    });
  }

  addDelete(collection: string, filter: Record<string, unknown>, database: string = 'test'): this {
    return this.addOperation({
      type: 'delete',
      collection,
      database,
      filter,
    });
  }

  withStartTime(timestamp: number | Date): this {
    this.txnState.startTime = timestamp instanceof Date ? timestamp.getTime() : timestamp;
    return this;
  }

  withCommitTime(timestamp: number | Date): this {
    this.txnState.commitTime = timestamp instanceof Date ? timestamp.getTime() : timestamp;
    return this;
  }

  build(): TransactionTestState {
    return {
      ...this.txnState,
      options: this.txnState.options ? { ...this.txnState.options } : null,
      operations: this.txnState.operations.map(op => ({ ...op })),
    };
  }
}

/**
 * Create a transaction test state.
 */
export function createTransactionState(overrides: Partial<TransactionTestState> = {}): TransactionTestState {
  const builder = new TransactionStateBuilder();
  if (overrides.sessionId) builder.withSessionId(overrides.sessionId);
  if (overrides.txnNumber !== undefined) builder.withTxnNumber(overrides.txnNumber);
  if (overrides.state) builder.withState(overrides.state);
  if (overrides.options) builder.withOptions(overrides.options);
  if (overrides.operations) builder.withOperations(overrides.operations);
  if (overrides.startTime) builder.withStartTime(overrides.startTime);
  if (overrides.commitTime) builder.withCommitTime(overrides.commitTime);
  return builder.build();
}

/**
 * Create a transaction in progress with operations.
 */
export function createInProgressTransaction(operations: Array<{
  type: 'insert' | 'update' | 'delete';
  collection: string;
  document?: Record<string, unknown>;
  filter?: Record<string, unknown>;
  update?: Record<string, unknown>;
}>): TransactionTestState {
  const builder = new TransactionStateBuilder().inProgress();

  for (const op of operations) {
    if (op.type === 'insert' && op.document) {
      builder.addInsert(op.collection, op.document);
    } else if (op.type === 'update' && op.filter && op.update) {
      builder.addUpdate(op.collection, op.filter, op.update);
    } else if (op.type === 'delete' && op.filter) {
      builder.addDelete(op.collection, op.filter);
    }
  }

  return builder.build();
}

// ============================================================================
// Index Entry Factory
// ============================================================================

/**
 * B-tree index entry for testing.
 */
export interface BTreeIndexEntry<K = unknown> {
  key: K;
  docIds: string[];
}

/**
 * Index metadata for testing.
 */
export interface IndexMetadata {
  name: string;
  field: string;
  unique: boolean;
  sparse: boolean;
  createdAt: string;
}

/**
 * Serialized B-tree for testing.
 */
export interface SerializedBTree<K = unknown> {
  name: string;
  field: string;
  minDegree: number;
  rootId: string | null;
  nodes: SerializedNode<K>[];
  unique: boolean;
}

/**
 * Serialized B-tree node for testing.
 */
export interface SerializedNode<K = unknown> {
  id: string;
  isLeaf: boolean;
  keys: K[];
  docIds: string[][];
  childIds: string[];
}

/**
 * Builder class for creating index metadata.
 */
export class IndexMetadataBuilder {
  private metadata: IndexMetadata;

  constructor(name?: string, field?: string) {
    docCounter++;
    this.metadata = {
      name: name ?? `index-${docCounter}`,
      field: field ?? 'field',
      unique: false,
      sparse: false,
      createdAt: new Date().toISOString(),
    };
  }

  withName(name: string): this {
    this.metadata.name = name;
    return this;
  }

  withField(field: string): this {
    this.metadata.field = field;
    return this;
  }

  unique(value: boolean = true): this {
    this.metadata.unique = value;
    return this;
  }

  sparse(value: boolean = true): this {
    this.metadata.sparse = value;
    return this;
  }

  withCreatedAt(date: string | Date): this {
    this.metadata.createdAt = date instanceof Date ? date.toISOString() : date;
    return this;
  }

  build(): IndexMetadata {
    return { ...this.metadata };
  }
}

/**
 * Builder class for creating B-tree index entries.
 */
export class BTreeIndexEntryBuilder<K = unknown> {
  private entry: BTreeIndexEntry<K>;

  constructor(key: K) {
    this.entry = {
      key,
      docIds: [],
    };
  }

  withDocIds(docIds: string[]): this {
    this.entry.docIds = [...docIds];
    return this;
  }

  addDocId(docId: string): this {
    if (!this.entry.docIds.includes(docId)) {
      this.entry.docIds.push(docId);
    }
    return this;
  }

  build(): BTreeIndexEntry<K> {
    return { ...this.entry, docIds: [...this.entry.docIds] };
  }
}

/**
 * Create an index metadata object.
 */
export function createIndexMetadata(overrides: Partial<IndexMetadata> = {}): IndexMetadata {
  const builder = new IndexMetadataBuilder(overrides.name, overrides.field);
  if (overrides.unique !== undefined) builder.unique(overrides.unique);
  if (overrides.sparse !== undefined) builder.sparse(overrides.sparse);
  if (overrides.createdAt) builder.withCreatedAt(overrides.createdAt);
  return builder.build();
}

/**
 * Create a B-tree index entry.
 */
export function createIndexEntry<K>(key: K, docIds: string[] = []): BTreeIndexEntry<K> {
  docCounter++;
  const defaultDocIds = docIds.length > 0 ? docIds : [`doc-${docCounter}`];
  return new BTreeIndexEntryBuilder(key).withDocIds(defaultDocIds).build();
}

/**
 * Create multiple index entries for testing range queries.
 */
export function createIndexEntries<K>(entries: Array<{ key: K; docIds?: string[] }>): BTreeIndexEntry<K>[] {
  return entries.map(({ key, docIds }) => createIndexEntry(key, docIds));
}

/**
 * Create a serialized B-tree node for testing.
 */
export function createSerializedNode<K>(overrides: Partial<SerializedNode<K>> & { keys: K[] }): SerializedNode<K> {
  docCounter++;
  return {
    id: overrides.id ?? crypto.randomUUID(),
    isLeaf: overrides.isLeaf ?? true,
    keys: overrides.keys,
    docIds: overrides.docIds ?? overrides.keys.map((_, i) => [`doc-${docCounter}-${i}`]),
    childIds: overrides.childIds ?? [],
  };
}

/**
 * Create a serialized B-tree for testing.
 */
export function createSerializedBTree<K>(
  name: string,
  field: string,
  entries: Array<{ key: K; docIds?: string[] }>,
  options: { unique?: boolean; minDegree?: number } = {}
): SerializedBTree<K> {
  const { unique = false, minDegree = 3 } = options;

  if (entries.length === 0) {
    return {
      name,
      field,
      minDegree,
      rootId: null,
      nodes: [],
      unique,
    };
  }

  const rootId = crypto.randomUUID();
  const node = createSerializedNode({
    id: rootId,
    keys: entries.map(e => e.key),
    docIds: entries.map((e, i) => e.docIds ?? [`doc-${i}`]),
    isLeaf: true,
  });

  return {
    name,
    field,
    minDegree,
    rootId,
    nodes: [node],
    unique,
  };
}

// ============================================================================
// Session Factory
// ============================================================================

/**
 * Session options for testing.
 */
export interface SessionOptions {
  defaultTransactionOptions?: TransactionOptions;
  causalConsistency?: boolean;
}

/**
 * Session test state for testing session management.
 */
export interface SessionTestState {
  id: string;
  options: SessionOptions;
  transactionState: TransactionState;
  txnNumber: number;
  operationCount: number;
  createdAt: Date;
  lastUsed: Date;
  hasEnded: boolean;
}

/**
 * Builder class for creating session test state.
 */
export class SessionStateBuilder {
  private state: SessionTestState;

  constructor() {
    const now = new Date();
    this.state = {
      id: crypto.randomUUID(),
      options: {},
      transactionState: 'none',
      txnNumber: 0,
      operationCount: 0,
      createdAt: now,
      lastUsed: now,
      hasEnded: false,
    };
  }

  withId(id: string): this {
    this.state.id = id;
    return this;
  }

  withOptions(options: SessionOptions): this {
    this.state.options = { ...options };
    return this;
  }

  withCausalConsistency(enabled: boolean = true): this {
    this.state.options.causalConsistency = enabled;
    return this;
  }

  withDefaultTransactionOptions(options: TransactionOptions): this {
    this.state.options.defaultTransactionOptions = { ...options };
    return this;
  }

  withTransactionState(state: TransactionState): this {
    this.state.transactionState = state;
    return this;
  }

  withTxnNumber(txnNumber: number): this {
    this.state.txnNumber = txnNumber;
    return this;
  }

  withOperationCount(count: number): this {
    this.state.operationCount = count;
    return this;
  }

  withCreatedAt(date: Date): this {
    this.state.createdAt = date;
    return this;
  }

  withLastUsed(date: Date): this {
    this.state.lastUsed = date;
    return this;
  }

  ended(value: boolean = true): this {
    this.state.hasEnded = value;
    return this;
  }

  build(): SessionTestState {
    return {
      ...this.state,
      options: { ...this.state.options },
      createdAt: new Date(this.state.createdAt),
      lastUsed: new Date(this.state.lastUsed),
    };
  }
}

/**
 * Create a session test state.
 */
export function createSessionState(overrides: Partial<SessionTestState> = {}): SessionTestState {
  const builder = new SessionStateBuilder();
  if (overrides.id) builder.withId(overrides.id);
  if (overrides.options) builder.withOptions(overrides.options);
  if (overrides.transactionState) builder.withTransactionState(overrides.transactionState);
  if (overrides.txnNumber !== undefined) builder.withTxnNumber(overrides.txnNumber);
  if (overrides.operationCount !== undefined) builder.withOperationCount(overrides.operationCount);
  if (overrides.createdAt) builder.withCreatedAt(overrides.createdAt);
  if (overrides.lastUsed) builder.withLastUsed(overrides.lastUsed);
  if (overrides.hasEnded !== undefined) builder.ended(overrides.hasEnded);
  return builder.build();
}

// ============================================================================
// Collection Factory
// ============================================================================

/**
 * Collection metadata for testing.
 */
export interface CollectionMetadata {
  name: string;
  database: string;
  documentCount: number;
  indexCount: number;
  totalSize: number;
  avgDocSize: number;
  createdAt: Date;
  updatedAt: Date;
}

/**
 * Builder class for creating collection metadata.
 */
export class CollectionMetadataBuilder {
  private metadata: CollectionMetadata;

  constructor(name?: string) {
    docCounter++;
    const now = new Date();
    this.metadata = {
      name: name ?? `collection-${docCounter}`,
      database: 'test',
      documentCount: 0,
      indexCount: 1, // _id index
      totalSize: 0,
      avgDocSize: 0,
      createdAt: now,
      updatedAt: now,
    };
  }

  withName(name: string): this {
    this.metadata.name = name;
    return this;
  }

  withDatabase(database: string): this {
    this.metadata.database = database;
    return this;
  }

  withDocumentCount(count: number): this {
    this.metadata.documentCount = count;
    return this;
  }

  withIndexCount(count: number): this {
    this.metadata.indexCount = count;
    return this;
  }

  withTotalSize(size: number): this {
    this.metadata.totalSize = size;
    if (this.metadata.documentCount > 0) {
      this.metadata.avgDocSize = size / this.metadata.documentCount;
    }
    return this;
  }

  withAvgDocSize(size: number): this {
    this.metadata.avgDocSize = size;
    return this;
  }

  withStats(documentCount: number, totalSize: number): this {
    this.metadata.documentCount = documentCount;
    this.metadata.totalSize = totalSize;
    this.metadata.avgDocSize = documentCount > 0 ? totalSize / documentCount : 0;
    return this;
  }

  withCreatedAt(date: Date): this {
    this.metadata.createdAt = date;
    return this;
  }

  withUpdatedAt(date: Date): this {
    this.metadata.updatedAt = date;
    return this;
  }

  build(): CollectionMetadata {
    return {
      ...this.metadata,
      createdAt: new Date(this.metadata.createdAt),
      updatedAt: new Date(this.metadata.updatedAt),
    };
  }
}

/**
 * Create collection metadata.
 */
export function createCollectionMetadata(overrides: Partial<CollectionMetadata> = {}): CollectionMetadata {
  const builder = new CollectionMetadataBuilder(overrides.name);
  if (overrides.database) builder.withDatabase(overrides.database);
  if (overrides.documentCount !== undefined) builder.withDocumentCount(overrides.documentCount);
  if (overrides.indexCount !== undefined) builder.withIndexCount(overrides.indexCount);
  if (overrides.totalSize !== undefined) builder.withTotalSize(overrides.totalSize);
  if (overrides.avgDocSize !== undefined) builder.withAvgDocSize(overrides.avgDocSize);
  if (overrides.createdAt) builder.withCreatedAt(overrides.createdAt);
  if (overrides.updatedAt) builder.withUpdatedAt(overrides.updatedAt);
  return builder.build();
}

// ============================================================================
// Auth User Factory
// ============================================================================

/**
 * Auth user for testing authentication.
 */
export interface AuthUser {
  id: string;
  username: string;
  email: string;
  passwordHash: string;
  roles: string[];
  permissions: string[];
  createdAt: Date;
  updatedAt: Date;
  lastLogin?: Date;
  isActive: boolean;
  metadata?: Record<string, unknown>;
}

/**
 * Builder class for creating auth users.
 */
export class AuthUserBuilder {
  private user: AuthUser;

  constructor(username?: string) {
    docCounter++;
    const now = new Date();
    this.user = {
      id: crypto.randomUUID(),
      username: username ?? `user-${docCounter}`,
      email: `user-${docCounter}@example.com`,
      passwordHash: `hashed-password-${docCounter}`,
      roles: ['user'],
      permissions: ['read'],
      createdAt: now,
      updatedAt: now,
      isActive: true,
    };
  }

  withId(id: string): this {
    this.user.id = id;
    return this;
  }

  withUsername(username: string): this {
    this.user.username = username;
    return this;
  }

  withEmail(email: string): this {
    this.user.email = email;
    return this;
  }

  withPasswordHash(hash: string): this {
    this.user.passwordHash = hash;
    return this;
  }

  withRoles(roles: string[]): this {
    this.user.roles = [...roles];
    return this;
  }

  addRole(role: string): this {
    if (!this.user.roles.includes(role)) {
      this.user.roles.push(role);
    }
    return this;
  }

  withPermissions(permissions: string[]): this {
    this.user.permissions = [...permissions];
    return this;
  }

  addPermission(permission: string): this {
    if (!this.user.permissions.includes(permission)) {
      this.user.permissions.push(permission);
    }
    return this;
  }

  asAdmin(): this {
    this.user.roles = ['admin', 'user'];
    this.user.permissions = ['read', 'write', 'delete', 'admin'];
    return this;
  }

  asReadOnly(): this {
    this.user.roles = ['readonly'];
    this.user.permissions = ['read'];
    return this;
  }

  withLastLogin(date: Date): this {
    this.user.lastLogin = date;
    return this;
  }

  active(value: boolean = true): this {
    this.user.isActive = value;
    return this;
  }

  inactive(): this {
    this.user.isActive = false;
    return this;
  }

  withMetadata(metadata: Record<string, unknown>): this {
    this.user.metadata = { ...metadata };
    return this;
  }

  withCreatedAt(date: Date): this {
    this.user.createdAt = date;
    return this;
  }

  withUpdatedAt(date: Date): this {
    this.user.updatedAt = date;
    return this;
  }

  build(): AuthUser {
    return {
      ...this.user,
      roles: [...this.user.roles],
      permissions: [...this.user.permissions],
      createdAt: new Date(this.user.createdAt),
      updatedAt: new Date(this.user.updatedAt),
      lastLogin: this.user.lastLogin ? new Date(this.user.lastLogin) : undefined,
      metadata: this.user.metadata ? { ...this.user.metadata } : undefined,
    };
  }
}

/**
 * Create an auth user.
 */
export function createAuthUser(overrides: Partial<AuthUser> = {}): AuthUser {
  const builder = new AuthUserBuilder(overrides.username);
  if (overrides.id) builder.withId(overrides.id);
  if (overrides.email) builder.withEmail(overrides.email);
  if (overrides.passwordHash) builder.withPasswordHash(overrides.passwordHash);
  if (overrides.roles) builder.withRoles(overrides.roles);
  if (overrides.permissions) builder.withPermissions(overrides.permissions);
  if (overrides.lastLogin) builder.withLastLogin(overrides.lastLogin);
  if (overrides.isActive !== undefined) builder.active(overrides.isActive);
  if (overrides.metadata) builder.withMetadata(overrides.metadata);
  if (overrides.createdAt) builder.withCreatedAt(overrides.createdAt);
  if (overrides.updatedAt) builder.withUpdatedAt(overrides.updatedAt);
  return builder.build();
}

/**
 * Create multiple auth users with different roles.
 */
export function createAuthUsers(count: number, role: 'admin' | 'user' | 'readonly' = 'user'): AuthUser[] {
  const users: AuthUser[] = [];

  for (let i = 0; i < count; i++) {
    const builder = new AuthUserBuilder();
    if (role === 'admin') builder.asAdmin();
    else if (role === 'readonly') builder.asReadOnly();
    users.push(builder.build());
  }

  return users;
}
