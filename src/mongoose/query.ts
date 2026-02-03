/**
 * MongoLake Mongoose Query Builder
 *
 * Provides Mongoose-compatible query building with:
 * - Chainable query methods
 * - Population support
 * - Lean queries
 * - Middleware execution
 */

import type { Model, PopulateOptions, LeanOptions } from './model.js';
// MongooseDocument type is reserved for potential future use with document hydration
// eslint-disable-next-line @typescript-eslint/no-unused-vars
import type { MongooseDocument as _MongooseDocument } from './document.js';
import type { Document as BaseDocument, Filter, Update } from '../types.js';

// ============================================================================
// Query Types
// ============================================================================

/**
 * Query operation type
 */
export type QueryOperation =
  | 'find'
  | 'findOne'
  | 'findOneAndUpdate'
  | 'findOneAndDelete'
  | 'findOneAndReplace'
  | 'updateOne'
  | 'updateMany'
  | 'deleteOne'
  | 'deleteMany'
  | 'countDocuments'
  | 'estimatedDocumentCount'
  | 'distinct'
  | 'replaceOne';

/**
 * Query options
 */
export interface QueryOptions {
  lean?: boolean | LeanOptions;
  populate?: PopulateOptions | PopulateOptions[];
  session?: unknown;
  projection?: Record<string, 0 | 1>;
  sort?: Record<string, 1 | -1>;
  limit?: number;
  skip?: number;
  hint?: string | Record<string, 1 | -1>;
  maxTimeMS?: number;
  new?: boolean;
  upsert?: boolean;
  rawResult?: boolean;
  timestamps?: boolean;
  update?: Update<BaseDocument> | unknown;
  replacement?: BaseDocument;
  field?: string;
  batchSize?: number;
  collation?: {
    locale: string;
    strength?: number;
    caseLevel?: boolean;
    caseFirst?: string;
    numericOrdering?: boolean;
  };
  readPreference?: string;
  comment?: string;
  explain?: boolean;
  tailable?: boolean;
}

// ============================================================================
// Query Class
// ============================================================================

/**
 * Mongoose-compatible Query class
 */
export class Query<ResultType, DocType extends BaseDocument = BaseDocument> implements PromiseLike<ResultType> {
  private _model: Model<DocType>;
  private _operation: QueryOperation;
  private _filter: Filter<DocType>;
  private _options: QueryOptions;
  private _populatePaths: PopulateOptions[] = [];
  private _lean: boolean | LeanOptions = false;
  // @ts-expect-error Reserved for future projection tracking
  private _selected: Set<string> | null = null;
  // @ts-expect-error Alias for backwards compatibility
  private _conditions: Filter<DocType>;

  constructor(
    model: Model<DocType>,
    operation: QueryOperation,
    filter?: Filter<DocType>,
    projection?: Record<string, 0 | 1>,
    options?: QueryOptions
  ) {
    this._model = model;
    this._operation = operation;
    this._filter = filter || ({} as Filter<DocType>);
    this._conditions = this._filter;
    this._options = {
      projection,
      ...options,
    };

    if (projection) {
      this._selected = new Set(Object.keys(projection).filter((k) => projection[k] === 1));
    }
  }

  // ============================================================================
  // Query Conditions
  // ============================================================================

  /**
   * Add filter conditions
   */
  where(path: string | Record<string, unknown>, val?: unknown): this {
    if (typeof path === 'object') {
      Object.assign(this._filter, path);
    } else if (val !== undefined) {
      (this._filter as Record<string, unknown>)[path] = val;
    }
    return this;
  }

  /**
   * Equals condition
   */
  equals(_val: unknown): this {
    // This is used after where() to set the value
    return this;
  }

  /**
   * Greater than
   */
  gt(path: string, val: unknown): this;
  gt(val: unknown): this;
  gt(pathOrVal: string | unknown, val?: unknown): this {
    return this.addOperator('$gt', pathOrVal, val);
  }

  /**
   * Greater than or equal
   */
  gte(path: string, val: unknown): this;
  gte(val: unknown): this;
  gte(pathOrVal: string | unknown, val?: unknown): this {
    return this.addOperator('$gte', pathOrVal, val);
  }

  /**
   * Less than
   */
  lt(path: string, val: unknown): this;
  lt(val: unknown): this;
  lt(pathOrVal: string | unknown, val?: unknown): this {
    return this.addOperator('$lt', pathOrVal, val);
  }

  /**
   * Less than or equal
   */
  lte(path: string, val: unknown): this;
  lte(val: unknown): this;
  lte(pathOrVal: string | unknown, val?: unknown): this {
    return this.addOperator('$lte', pathOrVal, val);
  }

  /**
   * Not equal
   */
  ne(path: string, val: unknown): this;
  ne(val: unknown): this;
  ne(pathOrVal: string | unknown, val?: unknown): this {
    return this.addOperator('$ne', pathOrVal, val);
  }

  /**
   * In array
   */
  in(path: string, vals: unknown[]): this;
  in(vals: unknown[]): this;
  in(pathOrVals: string | unknown[], vals?: unknown[]): this {
    if (Array.isArray(pathOrVals)) {
      return this.addOperator('$in', pathOrVals);
    }
    return this.addOperator('$in', pathOrVals, vals);
  }

  /**
   * Not in array
   */
  nin(path: string, vals: unknown[]): this;
  nin(vals: unknown[]): this;
  nin(pathOrVals: string | unknown[], vals?: unknown[]): this {
    if (Array.isArray(pathOrVals)) {
      return this.addOperator('$nin', pathOrVals);
    }
    return this.addOperator('$nin', pathOrVals, vals);
  }

  /**
   * Exists
   */
  exists(path: string, val?: boolean): this;
  exists(val?: boolean): this;
  exists(pathOrVal?: string | boolean, val?: boolean): this {
    if (typeof pathOrVal === 'boolean' || pathOrVal === undefined) {
      return this.addOperator('$exists', pathOrVal ?? true);
    }
    return this.addOperator('$exists', pathOrVal, val ?? true);
  }

  /**
   * Regex match
   */
  regex(path: string, val: RegExp | string): this;
  regex(val: RegExp | string): this;
  regex(pathOrVal: string | RegExp, val?: RegExp | string): this {
    if (val !== undefined) {
      return this.addOperator('$regex', pathOrVal as string, val);
    }
    return this.addOperator('$regex', pathOrVal);
  }

  /**
   * Size (array length)
   */
  size(path: string, val: number): this;
  size(val: number): this;
  size(pathOrVal: string | number, val?: number): this {
    if (typeof pathOrVal === 'number') {
      return this.addOperator('$size', pathOrVal);
    }
    return this.addOperator('$size', pathOrVal, val);
  }

  /**
   * All (array contains all)
   */
  all(path: string, vals: unknown[]): this;
  all(vals: unknown[]): this;
  all(pathOrVals: string | unknown[], vals?: unknown[]): this {
    if (Array.isArray(pathOrVals)) {
      return this.addOperator('$all', pathOrVals);
    }
    return this.addOperator('$all', pathOrVals, vals);
  }

  /**
   * Element match
   */
  elemMatch(path: string, criteria: Record<string, unknown>): this;
  elemMatch(criteria: Record<string, unknown>): this;
  elemMatch(pathOrCriteria: string | Record<string, unknown>, criteria?: Record<string, unknown>): this {
    if (typeof pathOrCriteria === 'object') {
      return this.addOperator('$elemMatch', pathOrCriteria);
    }
    return this.addOperator('$elemMatch', pathOrCriteria, criteria);
  }

  /**
   * Or condition
   */
  or(conditions: Filter<DocType>[]): this {
    (this._filter as Record<string, unknown>)['$or'] = conditions;
    return this;
  }

  /**
   * And condition
   */
  and(conditions: Filter<DocType>[]): this {
    (this._filter as Record<string, unknown>)['$and'] = conditions;
    return this;
  }

  /**
   * Nor condition
   */
  nor(conditions: Filter<DocType>[]): this {
    (this._filter as Record<string, unknown>)['$nor'] = conditions;
    return this;
  }

  private _lastPath: string | null = null;

  private addOperator(op: string, pathOrVal: unknown, val?: unknown): this {
    if (val !== undefined && typeof pathOrVal === 'string') {
      // Two-argument form: addOperator('$gt', 'age', 18)
      const filter = this._filter as Record<string, Record<string, unknown>>;
      if (!filter[pathOrVal]) {
        filter[pathOrVal] = {};
      }
      filter[pathOrVal][op] = val;
    } else {
      // One-argument form: after where()
      if (this._lastPath) {
        const filter = this._filter as Record<string, Record<string, unknown>>;
        if (!filter[this._lastPath]) {
          filter[this._lastPath] = {};
        }
        filter[this._lastPath]![op] = pathOrVal;
      }
    }
    return this;
  }

  // ============================================================================
  // Query Options
  // ============================================================================

  /**
   * Set projection
   */
  select(fields: string | Record<string, 0 | 1> | string[]): this {
    if (typeof fields === 'string') {
      const projection: Record<string, 0 | 1> = {};
      for (const field of fields.split(/\s+/)) {
        if (field.startsWith('-')) {
          projection[field.slice(1)] = 0;
        } else if (field.startsWith('+')) {
          projection[field.slice(1)] = 1;
        } else if (field) {
          projection[field] = 1;
        }
      }
      this._options.projection = projection;
    } else if (Array.isArray(fields)) {
      const projection: Record<string, 0 | 1> = {};
      for (const field of fields) {
        projection[field] = 1;
      }
      this._options.projection = projection;
    } else {
      this._options.projection = fields;
    }
    return this;
  }

  /**
   * Set sort order
   */
  sort(spec: string | Record<string, 1 | -1> | [string, 1 | -1][]): this {
    if (typeof spec === 'string') {
      const sort: Record<string, 1 | -1> = {};
      for (const field of spec.split(/\s+/)) {
        if (field.startsWith('-')) {
          sort[field.slice(1)] = -1;
        } else if (field) {
          sort[field] = 1;
        }
      }
      this._options.sort = sort;
    } else if (Array.isArray(spec)) {
      const sort: Record<string, 1 | -1> = {};
      for (const [field, dir] of spec) {
        sort[field] = dir;
      }
      this._options.sort = sort;
    } else {
      this._options.sort = spec;
    }
    return this;
  }

  /**
   * Limit results
   */
  limit(n: number): this {
    this._options.limit = n;
    return this;
  }

  /**
   * Skip results
   */
  skip(n: number): this {
    this._options.skip = n;
    return this;
  }

  /**
   * Set batch size
   */
  batchSize(n: number): this {
    this._options.batchSize = n;
    return this;
  }

  /**
   * Add comment
   */
  comment(str: string): this {
    this._options.comment = str;
    return this;
  }

  /**
   * Set hint
   */
  hint(spec: string | Record<string, 1 | -1>): this {
    this._options.hint = spec;
    return this;
  }

  /**
   * Set max time
   */
  maxTimeMS(ms: number): this {
    this._options.maxTimeMS = ms;
    return this;
  }

  /**
   * Set collation
   */
  collation(options: QueryOptions['collation']): this {
    this._options.collation = options;
    return this;
  }

  /**
   * Set read preference
   */
  read(pref: string): this {
    this._options.readPreference = pref;
    return this;
  }

  /**
   * Enable lean mode
   */
  lean(enable: boolean | LeanOptions = true): this {
    this._lean = enable;
    return this;
  }

  /**
   * Set session
   */
  session(sess: unknown): this {
    this._options.session = sess;
    return this;
  }

  /**
   * Explain the query
   */
  explain(_verbosity?: string): this {
    this._options.explain = true;
    return this;
  }

  // ============================================================================
  // Population
  // ============================================================================

  /**
   * Add population
   */
  populate(path: string | PopulateOptions | (string | PopulateOptions)[]): this {
    const paths = Array.isArray(path) ? path : [path];

    for (const p of paths) {
      if (typeof p === 'string') {
        this._populatePaths.push({ path: p });
      } else {
        this._populatePaths.push(p);
      }
    }

    return this;
  }

  // ============================================================================
  // Update Operations
  // ============================================================================

  /**
   * Set update operation
   */
  setUpdate(update: Update<DocType>): this {
    this._options.update = update;
    return this;
  }

  /**
   * Set option for findOneAndUpdate
   */
  setOptions(options: Partial<QueryOptions>): this {
    Object.assign(this._options, options);
    return this;
  }

  // ============================================================================
  // Execution
  // ============================================================================

  /**
   * Get the filter
   */
  getFilter(): Filter<DocType> {
    return this._filter;
  }

  /**
   * Get the query conditions (alias for getFilter)
   */
  getQuery(): Filter<DocType> {
    return this._filter;
  }

  /**
   * Get the options
   */
  getOptions(): QueryOptions {
    return this._options;
  }

  /**
   * Get the update
   */
  getUpdate(): Update<DocType> | undefined {
    return this._options.update as Update<DocType> | undefined;
  }

  /**
   * Merge another query
   */
  merge(source: Query<unknown, DocType>): this {
    Object.assign(this._filter, source._filter);
    Object.assign(this._options, source._options);
    return this;
  }

  /**
   * Execute the query
   */
  async exec(): Promise<ResultType> {
    // Run pre middleware
    await this._model.schema.runPreMiddleware(this._operation as 'find', this);

    let result: unknown;

    switch (this._operation) {
      case 'find':
        result = await this.execFind();
        break;

      case 'findOne':
        result = await this.execFindOne();
        break;

      case 'findOneAndUpdate':
        result = await this.execFindOneAndUpdate();
        break;

      case 'findOneAndDelete':
        result = await this.execFindOneAndDelete();
        break;

      case 'updateOne':
        result = await this._model.collection.updateOne(
          this._filter,
          this._options.update as Update<DocType>,
          { upsert: this._options.upsert }
        );
        break;

      case 'updateMany':
        result = await this._model.collection.updateMany(
          this._filter,
          this._options.update as Update<DocType>,
          { upsert: this._options.upsert }
        );
        break;

      case 'deleteOne':
        result = await this._model.collection.deleteOne(this._filter);
        break;

      case 'deleteMany':
        result = await this._model.collection.deleteMany(this._filter);
        break;

      case 'countDocuments':
        result = await this._model.collection.countDocuments(this._filter);
        break;

      case 'estimatedDocumentCount':
        result = await this._model.collection.estimatedDocumentCount();
        break;

      case 'distinct':
        result = await this._model.collection.distinct(
          this._options.field as keyof DocType,
          this._filter
        );
        break;

      case 'replaceOne':
        result = await this._model.collection.replaceOne(
          this._filter,
          this._options.replacement as DocType,
          { upsert: this._options.upsert }
        );
        break;

      default:
        throw new Error(`Unknown query operation: ${this._operation}`);
    }

    // Run post middleware
    await this._model.schema.runPostMiddleware(this._operation as 'find', this, result);

    return result as ResultType;
  }

  private async execFind(): Promise<unknown> {
    const cursor = this._model.collection.find(this._filter, {
      projection: this._options.projection,
      sort: this._options.sort,
      limit: this._options.limit,
      skip: this._options.skip,
      hint: this._options.hint,
      maxTimeMS: this._options.maxTimeMS,
    });

    let docs: DocType[] = await cursor.toArray() as DocType[];

    // Handle population
    if (this._populatePaths.length > 0) {
      docs = await this._model.populate(docs, this._populatePaths) as DocType[];
    }

    // Handle lean mode
    if (this._lean) {
      return docs;
    }

    // Hydrate to documents
    return docs.map((doc) => {
      const mongooseDoc = this._model.hydrate(doc);
      mongooseDoc.isNew = false;
      return mongooseDoc;
    });
  }

  private async execFindOne(): Promise<unknown> {
    const doc = await this._model.collection.findOne(this._filter, {
      projection: this._options.projection,
      hint: this._options.hint,
      maxTimeMS: this._options.maxTimeMS,
    });

    if (!doc) return null;

    // Handle population
    let result: DocType = doc as DocType;
    if (this._populatePaths.length > 0) {
      result = await this._model.populate(doc as DocType, this._populatePaths) as DocType;
    }

    // Handle lean mode
    if (this._lean) {
      return result;
    }

    // Hydrate to document
    const mongooseDoc = this._model.hydrate(result);
    mongooseDoc.isNew = false;
    return mongooseDoc;
  }

  private async execFindOneAndUpdate(): Promise<unknown> {
    // First find the document
    const existing = await this._model.collection.findOne(this._filter);

    if (!existing && !this._options.upsert) {
      return null;
    }

    // Apply update
    await this._model.collection.updateOne(
      this._filter,
      this._options.update as Update<DocType>,
      { upsert: this._options.upsert }
    );

    // Return old or new document based on options
    if (this._options.new) {
      const updated = await this._model.collection.findOne(this._filter);
      if (!updated) return null;

      if (this._lean) return updated;
      const mongooseDoc = this._model.hydrate(updated);
      mongooseDoc.isNew = false;
      return mongooseDoc;
    }

    if (!existing) return null;
    if (this._lean) return existing;
    const mongooseDoc = this._model.hydrate(existing);
    mongooseDoc.isNew = false;
    return mongooseDoc;
  }

  private async execFindOneAndDelete(): Promise<unknown> {
    // Find the document first
    const doc = await this._model.collection.findOne(this._filter, {
      projection: this._options.projection,
    });

    if (!doc) return null;

    // Delete it - Filter type { _id: string } is compatible with Filter<DocType>.
    await this._model.collection.deleteOne({ _id: doc._id } as Filter<DocType>);

    // Handle lean mode
    if (this._lean) {
      return doc;
    }

    // Hydrate to document
    const mongooseDoc = this._model.hydrate(doc);
    mongooseDoc.isNew = false;
    mongooseDoc.$isDeleted(true);
    return mongooseDoc;
  }

  // ============================================================================
  // Promise Interface
  // ============================================================================

  /**
   * Then handler
   */
  then<TResult1 = ResultType, TResult2 = never>(
    onfulfilled?: ((value: ResultType) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): Promise<TResult1 | TResult2> {
    return this.exec().then(onfulfilled, onrejected);
  }

  /**
   * Catch handler
   */
  catch<TResult = never>(
    onrejected?: ((reason: unknown) => TResult | PromiseLike<TResult>) | null
  ): Promise<ResultType | TResult> {
    return this.exec().catch(onrejected);
  }

  /**
   * Finally handler
   */
  finally(onfinally?: (() => void) | null): Promise<ResultType> {
    return this.exec().finally(onfinally);
  }

  // ============================================================================
  // Cursor Methods
  // ============================================================================

  /**
   * Get cursor
   */
  cursor(): AsyncIterable<DocType> {
    return this._model.collection.find(this._filter, {
      projection: this._options.projection,
      sort: this._options.sort,
      limit: this._options.limit,
      skip: this._options.skip,
    });
  }

  /**
   * Transform documents in stream
   */
  transform<R>(fn: (doc: DocType) => R): TransformQuery<R, DocType> {
    // Single cast is safe - Query type parameter constraints allow this assignment.
    return new TransformQuery(this as Query<DocType[], DocType>, fn);
  }

  // ============================================================================
  // Utility Methods
  // ============================================================================

  /**
   * Cast values according to schema
   */
  cast(_model?: Model<DocType>): this {
    // Schema casting would be applied during execution
    return this;
  }

  /**
   * Clone the query
   */
  clone(): Query<ResultType, DocType> {
    const cloned = new Query<ResultType, DocType>(
      this._model,
      this._operation,
      { ...this._filter },
      this._options.projection,
      { ...this._options }
    );
    cloned._populatePaths = [...this._populatePaths];
    cloned._lean = this._lean;
    return cloned;
  }

  /**
   * Get model
   */
  model(): Model<DocType> {
    return this._model;
  }
}

/**
 * Transform query wrapper
 */
class TransformQuery<R, DocType extends BaseDocument> {
  constructor(
    private _query: Query<DocType[], DocType>,
    private _transform: (doc: DocType) => R
  ) {}

  async exec(): Promise<R[]> {
    const docs = await this._query.exec();
    return docs.map(this._transform);
  }

  then<TResult1 = R[], TResult2 = never>(
    onfulfilled?: ((value: R[]) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): Promise<TResult1 | TResult2> {
    return this.exec().then(onfulfilled, onrejected);
  }
}
