/**
 * MongoLake Mongoose Document
 *
 * Provides Mongoose-compatible Document class with:
 * - Data access and modification
 * - Validation
 * - Middleware execution
 * - Save/remove operations
 * - Change tracking
 */

import { Schema } from './schema.js';
import { ObjectId, type Document as BaseDocument, type Filter } from '../types.js';
import type { Model, PopulateOptions } from './model.js';

// ============================================================================
// Document Types
// ============================================================================

/**
 * Document methods interface
 */
export interface DocumentMethods {
  save(options?: SaveOptions): Promise<this>;
  remove(): Promise<this>;
  deleteOne(): Promise<this>;
  validate(pathsToValidate?: string[]): Promise<void>;
  validateSync(pathsToValidate?: string[]): ValidationError | undefined;
  invalidate(path: string, errorMsg: string | Error, value?: unknown): ValidationError;
  populate(path: string | PopulateOptions | (string | PopulateOptions)[]): Promise<this>;
  depopulate(path?: string): this;
  toObject(options?: ToObjectOptions): Record<string, unknown>;
  toJSON(options?: ToObjectOptions): Record<string, unknown>;
  get(path: string): unknown;
  set(path: string, value: unknown): this;
  set(obj: Record<string, unknown>): this;
  unset(path: string): this;
  markModified(path: string): void;
  isModified(path?: string | string[]): boolean;
  isDirectModified(path: string): boolean;
  isNew: boolean;
  isSelected(path: string): boolean;
  isInit(path: string): boolean;
  equals(doc: MongooseDocument<BaseDocument>): boolean;
  increment(): this;
  $isDefault(path: string): boolean;
  $isEmpty(path?: string): boolean;
  $isDeleted(val?: boolean): boolean;
  $ignore(path: string): void;
  $clone(): this;
  $model(name?: string): Model<BaseDocument>;
  errors?: ValidationError;
  _doc: Record<string, unknown>;
  id?: string;
  _id?: string | ObjectId;
  schema: Schema;
  collection: { name: string };
  $locals: Record<string, unknown>;
  $op: string | null;
  $where: Record<string, unknown>;
}

/**
 * Save options
 */
export interface SaveOptions {
  validateBeforeSave?: boolean;
  validateModifiedOnly?: boolean;
  timestamps?: boolean;
  session?: unknown;
  safe?: boolean;
}

/**
 * ToObject/ToJSON options
 */
export interface ToObjectOptions {
  getters?: boolean;
  virtuals?: boolean;
  minimize?: boolean;
  transform?: (doc: unknown, ret: Record<string, unknown>, options: ToObjectOptions) => unknown;
  depopulate?: boolean;
  versionKey?: boolean;
  flattenMaps?: boolean;
  flattenObjectIds?: boolean;
  useProjection?: boolean;
}

/**
 * Validation error
 */
export class ValidationError extends Error {
  public errors: Record<string, ValidatorError>;
  public _message: string;

  constructor(message?: string) {
    super(message || 'Validation failed');
    this.name = 'ValidationError';
    this._message = message || 'Validation failed';
    this.errors = {};
  }

  /**
   * Add an error for a path
   */
  addError(path: string, error: ValidatorError): void {
    this.errors[path] = error;
    this.message = this.buildMessage();
  }

  private buildMessage(): string {
    const paths = Object.keys(this.errors);
    if (paths.length === 0) return this._message;

    const messages = paths.map((path) => {
      const error = this.errors[path]!;
      return `${path}: ${error.message}`;
    });

    return `${this._message}: ${messages.join(', ')}`;
  }

  /**
   * Convert to string
   */
  toString(): string {
    return this.message;
  }
}

/**
 * Single validator error
 */
export class ValidatorError extends Error {
  public kind: string;
  public path: string;
  public value: unknown;
  public reason?: Error;
  public properties: {
    message: string;
    type: string;
    path: string;
    value: unknown;
    reason?: Error;
  };

  constructor(options: {
    message: string;
    kind: string;
    path: string;
    value?: unknown;
    reason?: Error;
  }) {
    super(options.message);
    this.name = 'ValidatorError';
    this.kind = options.kind;
    this.path = options.path;
    this.value = options.value;
    this.reason = options.reason;
    this.properties = {
      message: options.message,
      type: options.kind,
      path: options.path,
      value: options.value,
      reason: options.reason,
    };
  }
}

/**
 * Cast error for type coercion failures
 */
export class CastError extends Error {
  public kind: string;
  public value: unknown;
  public path: string;
  public reason?: Error;
  public model?: string;

  constructor(type: string, value: unknown, path: string, reason?: Error, model?: string) {
    const valueStr = value === null ? 'null' : JSON.stringify(value);
    super(`Cast to ${type} failed for value "${valueStr}" at path "${path}"`);
    this.name = 'CastError';
    this.kind = type;
    this.value = value;
    this.path = path;
    this.reason = reason;
    this.model = model;
  }
}

// ============================================================================
// Document Class
// ============================================================================

/**
 * Mongoose-compatible Document class
 */
export class MongooseDocument<T extends BaseDocument = BaseDocument> implements DocumentMethods {
  public _doc: Record<string, unknown>;
  public schema: Schema<T>;
  public isNew: boolean = true;
  public errors?: ValidationError;
  public $locals: Record<string, unknown> = {};
  public $op: string | null = null;
  public $where: Record<string, unknown> = {};

  private _model: Model<T>;
  private _modifiedPaths: Set<string> = new Set();
  private _populated: Map<string, unknown> = new Map();
  private _selected: Set<string> | null = null;
  private _isDeleted: boolean = false;
  private _ignoredPaths: Set<string> = new Set();

  constructor(doc: Partial<T> | undefined, schema: Schema<T>, model: Model<T>) {
    this.schema = schema;
    this._model = model;
    this._doc = {};

    // Apply defaults
    this.applyDefaults();

    // Set provided values
    if (doc) {
      for (const [key, value] of Object.entries(doc)) {
        this.set(key, value);
      }
    }

    // Clear modification tracking after initial set
    this._modifiedPaths.clear();
  }

  /**
   * Apply schema defaults
   */
  private applyDefaults(): void {
    for (const [path, schemaPath] of this.schema.paths) {
      if (schemaPath.defaultValue !== undefined && this._doc[path] === undefined) {
        this._doc[path] = schemaPath.getDefault();
      }
    }
  }

  /**
   * Get collection name
   */
  get collection(): { name: string } {
    return { name: this._model.collection.name };
  }

  /**
   * Get document ID
   */
  get id(): string | undefined {
    const id = this._doc._id;
    return id ? String(id) : undefined;
  }

  /**
   * Get _id
   */
  get _id(): string | ObjectId | undefined {
    return this._doc._id as string | ObjectId | undefined;
  }

  /**
   * Set _id
   */
  set _id(value: string | ObjectId | undefined) {
    this._doc._id = value;
    this._modifiedPaths.add('_id');
  }

  /**
   * Get the model for this document
   */
  $model(name?: string): Model<BaseDocument> {
    if (name) {
      // Return a different model by name
      const { getModel } = require('./model.js');
      return getModel(name);
    }
    // Model<T> where T extends BaseDocument should be assignable to Model<BaseDocument>,
    // but TypeScript's strict variance checking requires the double cast.
    return this._model as unknown as Model<BaseDocument>;
  }

  /**
   * Get a field value
   */
  get(path: string): unknown {
    const parts = path.split('.');
    let current: unknown = this._doc;

    for (const part of parts) {
      if (current == null || typeof current !== 'object') {
        return undefined;
      }
      current = (current as Record<string, unknown>)[part];
    }

    // Apply getters from schema
    const schemaPath = this.schema.paths.get(path);
    if (schemaPath) {
      return schemaPath.applyGetters(current);
    }

    return current;
  }

  /**
   * Set a field value
   */
  set(path: string, value: unknown): this;
  set(obj: Record<string, unknown>): this;
  set(pathOrObj: string | Record<string, unknown>, value?: unknown): this {
    if (typeof pathOrObj === 'object') {
      for (const [key, val] of Object.entries(pathOrObj)) {
        this.set(key, val);
      }
      return this;
    }

    const path = pathOrObj;

    // Check strict mode
    if (this.schema.options.strict && !this.schema.paths.has(path)) {
      const pathType = this.schema.pathType(path);
      if (pathType === 'adhocOrUndefined') {
        if (this.schema.options.strict === 'throw') {
          throw new Error(`Field \`${path}\` is not in schema and strict mode is enabled`);
        }
        return this;
      }
    }

    // Apply setters from schema
    const schemaPath = this.schema.paths.get(path);
    let finalValue = value;
    if (schemaPath) {
      finalValue = schemaPath.applySetters(value);
      // Cast to correct type
      try {
        finalValue = schemaPath.cast(finalValue);
      } catch (err) {
        // Cast failed, will be caught in validation
      }
    }

    // Set nested path
    const parts = path.split('.');
    if (parts.length === 1) {
      this._doc[path] = finalValue;
    } else {
      let current = this._doc;
      for (let i = 0; i < parts.length - 1; i++) {
        const part = parts[i]!;
        if (current[part] == null || typeof current[part] !== 'object') {
          current[part] = {};
        }
        current = current[part] as Record<string, unknown>;
      }
      current[parts[parts.length - 1]!] = finalValue;
    }

    // Mark as modified
    this._modifiedPaths.add(path);

    return this;
  }

  /**
   * Unset a field
   */
  unset(path: string): this {
    const parts = path.split('.');
    if (parts.length === 1) {
      delete this._doc[path];
    } else {
      let current: unknown = this._doc;
      for (let i = 0; i < parts.length - 1; i++) {
        if (current == null || typeof current !== 'object') return this;
        current = (current as Record<string, unknown>)[parts[i]!];
      }
      if (current && typeof current === 'object') {
        delete (current as Record<string, unknown>)[parts[parts.length - 1]!];
      }
    }

    this._modifiedPaths.add(path);
    return this;
  }

  /**
   * Mark a path as modified
   */
  markModified(path: string): void {
    this._modifiedPaths.add(path);
  }

  /**
   * Check if document or path is modified
   */
  isModified(path?: string | string[]): boolean {
    if (path === undefined) {
      return this._modifiedPaths.size > 0;
    }

    const paths = Array.isArray(path) ? path : [path];
    return paths.some((p) => {
      if (this._modifiedPaths.has(p)) return true;
      // Check if any nested paths are modified
      for (const modified of this._modifiedPaths) {
        if (modified.startsWith(p + '.')) return true;
      }
      return false;
    });
  }

  /**
   * Check if a specific path was directly modified
   */
  isDirectModified(path: string): boolean {
    return this._modifiedPaths.has(path);
  }

  /**
   * Check if path was selected
   */
  isSelected(path: string): boolean {
    if (this._selected === null) return true;
    return this._selected.has(path);
  }

  /**
   * Check if path was initialized
   */
  isInit(path: string): boolean {
    return this._doc[path] !== undefined;
  }

  /**
   * Check if path has default value
   */
  $isDefault(path: string): boolean {
    const schemaPath = this.schema.paths.get(path);
    if (!schemaPath || schemaPath.defaultValue === undefined) return false;

    const defaultValue = schemaPath.getDefault();
    const currentValue = this.get(path);

    return JSON.stringify(defaultValue) === JSON.stringify(currentValue);
  }

  /**
   * Check if document/path is empty
   */
  $isEmpty(path?: string): boolean {
    if (path) {
      const value = this.get(path);
      return value == null || (typeof value === 'object' && Object.keys(value as object).length === 0);
    }
    return Object.keys(this._doc).filter((k) => k !== '_id').length === 0;
  }

  /**
   * Get/set deleted state
   */
  $isDeleted(val?: boolean): boolean {
    if (val !== undefined) {
      this._isDeleted = val;
    }
    return this._isDeleted;
  }

  /**
   * Ignore a path during validation
   */
  $ignore(path: string): void {
    this._ignoredPaths.add(path);
  }

  /**
   * Clone the document
   */
  $clone(): this {
    const cloned = new MongooseDocument<T>(
      JSON.parse(JSON.stringify(this._doc)) as Partial<T>,
      this.schema,
      this._model
    );
    cloned.isNew = this.isNew;
    return cloned as this;
  }

  /**
   * Check equality with another document
   */
  equals(doc: MongooseDocument<BaseDocument>): boolean {
    if (!doc._id || !this._id) return false;
    return String(this._id) === String(doc._id);
  }

  /**
   * Increment version key
   */
  increment(): this {
    const versionKey = this.schema.options.versionKey;
    if (versionKey && typeof versionKey === 'string') {
      const current = (this._doc[versionKey] as number) || 0;
      this._doc[versionKey] = current + 1;
    }
    return this;
  }

  /**
   * Validate the document
   */
  async validate(pathsToValidate?: string[]): Promise<void> {
    const validationError = new ValidationError(`Validation failed`);
    const paths = pathsToValidate || Array.from(this.schema.paths.keys());

    await this.schema.runPreMiddleware('validate', this);

    for (const path of paths) {
      if (this._ignoredPaths.has(path)) continue;

      const schemaPath = this.schema.paths.get(path);
      if (!schemaPath) continue;

      const value = this.get(path);
      const result = await schemaPath.validate(value);

      if (!result.valid) {
        for (const error of result.errors) {
          validationError.addError(
            path,
            new ValidatorError({
              message: error,
              kind: 'user defined',
              path,
              value,
            })
          );
        }
      }
    }

    if (Object.keys(validationError.errors).length > 0) {
      this.errors = validationError;
      throw validationError;
    }

    await this.schema.runPostMiddleware('validate', this, this);
  }

  /**
   * Synchronous validation
   */
  validateSync(pathsToValidate?: string[]): ValidationError | undefined {
    const validationError = new ValidationError(`Validation failed`);
    const paths = pathsToValidate || Array.from(this.schema.paths.keys());

    for (const path of paths) {
      if (this._ignoredPaths.has(path)) continue;

      const schemaPath = this.schema.paths.get(path);
      if (!schemaPath) continue;

      const value = this.get(path);

      // Run synchronous validators only
      for (const validator of schemaPath.validators) {
        try {
          const result = validator.validator(value);
          // Skip async validators
          if (result instanceof Promise) continue;

          if (!result) {
            const msg =
              typeof validator.message === 'function'
                ? validator.message({ value, path })
                : validator.message || `Validation failed for path \`${path}\``;

            validationError.addError(
              path,
              new ValidatorError({
                message: msg.replace('{VALUE}', String(value)),
                kind: 'user defined',
                path,
                value,
              })
            );
          }
        } catch (err) {
          validationError.addError(
            path,
            new ValidatorError({
              message: `Validation error: ${err}`,
              kind: 'user defined',
              path,
              value,
            })
          );
        }
      }
    }

    if (Object.keys(validationError.errors).length > 0) {
      this.errors = validationError;
      return validationError;
    }

    return undefined;
  }

  /**
   * Manually invalidate a path
   */
  invalidate(path: string, errorMsg: string | Error, value?: unknown): ValidationError {
    if (!this.errors) {
      this.errors = new ValidationError('Validation failed');
    }

    const message = errorMsg instanceof Error ? errorMsg.message : errorMsg;
    this.errors.addError(
      path,
      new ValidatorError({
        message,
        kind: 'user defined',
        path,
        value: value ?? this.get(path),
        reason: errorMsg instanceof Error ? errorMsg : undefined,
      })
    );

    return this.errors;
  }

  /**
   * Save the document
   */
  async save(options?: SaveOptions): Promise<this> {
    const validateBeforeSave = options?.validateBeforeSave ?? true;

    this.$op = 'save';

    try {
      // Run pre save middleware
      await this.schema.runPreMiddleware('save', this);

      // Validate
      if (validateBeforeSave) {
        await this.validate(options?.validateModifiedOnly ? Array.from(this._modifiedPaths) : undefined);
      }

      // Update timestamps
      if (this.schema.options.timestamps && options?.timestamps !== false) {
        const updatedAt =
          typeof this.schema.options.timestamps === 'object' && this.schema.options.timestamps.updatedAt
            ? this.schema.options.timestamps.updatedAt === true
              ? 'updatedAt'
              : this.schema.options.timestamps.updatedAt
            : 'updatedAt';

        if (updatedAt) {
          this._doc[updatedAt] = new Date();
        }
      }

      if (this.isNew) {
        // Insert new document
        await this._model.collection.insertOne(this._doc as T);
        this.isNew = false;
      } else {
        // Update existing document
        this.increment();
        // Filter type { _id: string } is compatible with Filter<T> where T extends Document.
        // Single cast is safe because all documents have an _id field.
        await this._model.collection.replaceOne({ _id: this._id } as Filter<T>, this._doc as T);
      }

      // Clear modification tracking
      this._modifiedPaths.clear();

      // Run post save middleware
      await this.schema.runPostMiddleware('save', this, this);

      return this;
    } finally {
      this.$op = null;
    }
  }

  /**
   * Remove the document
   */
  async remove(): Promise<this> {
    this.$op = 'remove';

    try {
      // Run pre remove middleware
      await this.schema.runPreMiddleware('remove', this);

      // Delete - Filter type { _id: string } is compatible with Filter<T>.
      await this._model.collection.deleteOne({ _id: this._id } as Filter<T>);
      this._isDeleted = true;

      // Run post remove middleware
      await this.schema.runPostMiddleware('remove', this, this);

      return this;
    } finally {
      this.$op = null;
    }
  }

  /**
   * Delete the document (alias for remove)
   */
  async deleteOne(): Promise<this> {
    this.$op = 'deleteOne';

    try {
      await this.schema.runPreMiddleware('deleteOne', this);
      // Filter type { _id: string } is compatible with Filter<T>.
      await this._model.collection.deleteOne({ _id: this._id } as Filter<T>);
      this._isDeleted = true;
      await this.schema.runPostMiddleware('deleteOne', this, this);

      return this;
    } finally {
      this.$op = null;
    }
  }

  /**
   * Populate referenced documents
   */
  async populate(path: string | PopulateOptions | (string | PopulateOptions)[]): Promise<this> {
    await this._model.populate(this._doc as T, path);
    return this;
  }

  /**
   * Depopulate a path
   */
  depopulate(path?: string): this {
    if (path) {
      this._populated.delete(path);
      // Restore to just the ID
      const schemaPath = this.schema.paths.get(path);
      if (schemaPath?.options.ref) {
        const value = this._doc[path];
        if (value && typeof value === 'object' && '_id' in (value as Record<string, unknown>)) {
          this._doc[path] = (value as Record<string, unknown>)._id;
        }
      }
    } else {
      // Depopulate all
      for (const [p] of this._populated) {
        this.depopulate(p);
      }
    }
    return this;
  }

  /**
   * Convert to plain object
   */
  toObject(options?: ToObjectOptions): Record<string, unknown> {
    const opts = {
      getters: false,
      virtuals: false,
      minimize: this.schema.options.minimize ?? true,
      versionKey: true,
      depopulate: false,
      ...this.schema.options.toObject,
      ...options,
    };

    let ret = { ...this._doc };

    // Apply getters
    if (opts.getters) {
      for (const [path, schemaPath] of this.schema.paths) {
        if (ret[path] !== undefined) {
          ret[path] = schemaPath.applyGetters(ret[path]);
        }
      }
    }

    // Apply virtuals
    if (opts.virtuals) {
      for (const [name, virtual] of this.schema.virtuals) {
        if (virtual.hasGetter()) {
          ret[name] = virtual.applyGetters(this._doc);
        }
      }
    }

    // Minimize empty objects
    if (opts.minimize) {
      ret = this.minimizeObject(ret);
    }

    // Remove version key if disabled
    const versionKey = this.schema.options.versionKey;
    if (!opts.versionKey && typeof versionKey === 'string') {
      delete ret[versionKey];
    }

    // Apply transform
    if (opts.transform) {
      return opts.transform(this, ret, opts) as Record<string, unknown>;
    }

    return ret;
  }

  private minimizeObject(obj: Record<string, unknown>): Record<string, unknown> {
    const result: Record<string, unknown> = {};

    for (const [key, value] of Object.entries(obj)) {
      if (value === undefined) continue;

      if (value && typeof value === 'object' && !Array.isArray(value) && !(value instanceof Date) && !(value instanceof ObjectId)) {
        const minimized = this.minimizeObject(value as Record<string, unknown>);
        if (Object.keys(minimized).length > 0) {
          result[key] = minimized;
        }
      } else {
        result[key] = value;
      }
    }

    return result;
  }

  /**
   * Convert to JSON
   */
  toJSON(options?: ToObjectOptions): Record<string, unknown> {
    const opts = {
      ...this.schema.options.toJSON,
      ...options,
    };
    return this.toObject(opts);
  }

  /**
   * Get modified paths
   */
  modifiedPaths(): string[] {
    return Array.from(this._modifiedPaths);
  }

  /**
   * Get direct modified paths (no nested)
   */
  directModifiedPaths(): string[] {
    return Array.from(this._modifiedPaths).filter((p) => !p.includes('.'));
  }

  /**
   * Check if this is a new document
   */
  isNewDocument(): boolean {
    return this.isNew;
  }

  /**
   * Overwrite the document
   */
  overwrite(obj: Partial<T>): this {
    // Clear existing data except _id
    const id = this._doc._id;
    this._doc = { _id: id };

    // Set new values
    for (const [key, value] of Object.entries(obj)) {
      if (key !== '_id') {
        this.set(key, value);
      }
    }

    return this;
  }

  /**
   * Update document without saving
   */
  $set(obj: Partial<T>): this {
    return this.set(obj as Record<string, unknown>);
  }

  /**
   * Get all paths that have been populated
   */
  populated(path?: string): string | undefined {
    if (path) {
      return this._populated.has(path) ? path : undefined;
    }
    return undefined;
  }

  /**
   * Execute a function if the document was modified
   */
  $__reset(): void {
    this._modifiedPaths.clear();
  }
}
