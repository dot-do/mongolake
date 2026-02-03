/**
 * MongoLake Mongoose Schema
 *
 * Provides Mongoose-compatible schema definition with:
 * - Schema type support (all MongoDB types)
 * - Virtual fields
 * - Instance methods
 * - Static methods
 * - Middleware hooks (pre/post)
 * - Plugin support
 * - Path validation
 */

import { ObjectId } from '../types.js';

// ============================================================================
// Schema Types
// ============================================================================

/**
 * Supported schema types matching MongoDB/Mongoose types
 */
export type SchemaType =
  | typeof String
  | typeof Number
  | typeof Boolean
  | typeof Date
  | typeof Buffer
  | typeof ObjectId
  | typeof Array
  | typeof Object
  | typeof Map
  | 'String'
  | 'Number'
  | 'Boolean'
  | 'Date'
  | 'Buffer'
  | 'ObjectId'
  | 'Mixed'
  | 'Array'
  | 'Map'
  | 'Decimal128'
  | 'UUID'
  | 'BigInt';

/**
 * Schema type definition for a field
 */
export interface SchemaTypeDefinition {
  type: SchemaType | SchemaDefinition | [SchemaType | SchemaDefinition];
  required?: boolean | [boolean, string];
  default?: unknown | (() => unknown);
  validate?: ValidatorFunction | ValidatorDefinition | ValidatorDefinition[];
  get?: (value: unknown) => unknown;
  set?: (value: unknown) => unknown;
  alias?: string;
  index?: boolean | 'text' | 'hashed' | '2dsphere';
  unique?: boolean;
  sparse?: boolean;
  immutable?: boolean;
  enum?: unknown[];
  min?: number | Date | [number | Date, string];
  max?: number | Date | [number | Date, string];
  minlength?: number | [number, string];
  maxlength?: number | [number, string];
  match?: RegExp | [RegExp, string];
  lowercase?: boolean;
  uppercase?: boolean;
  trim?: boolean;
  ref?: string;
  select?: boolean;
  of?: SchemaTypeDefinition;
  transform?: (doc: unknown, ret: unknown) => unknown;
}

/**
 * Schema definition - map of field names to types
 */
export interface SchemaDefinition {
  [path: string]: SchemaType | SchemaTypeDefinition | SchemaDefinition | [SchemaType | SchemaTypeDefinition];
}

/**
 * Validator function signature
 */
export type ValidatorFunction = (value: unknown) => boolean | Promise<boolean>;

/**
 * Validator definition object
 */
export interface ValidatorDefinition {
  validator: ValidatorFunction;
  message?: string | ((props: { value: unknown; path: string }) => string);
}

/**
 * Schema options
 */
export interface SchemaOptions {
  /** Auto-generate timestamps (createdAt, updatedAt) */
  timestamps?: boolean | { createdAt?: string | boolean; updatedAt?: string | boolean };
  /** Collection name override */
  collection?: string;
  /** Auto-index on connect */
  autoIndex?: boolean;
  /** Auto-create collection */
  autoCreate?: boolean;
  /** Enable _id field */
  _id?: boolean;
  /** Enable __v (version key) */
  versionKey?: boolean | string;
  /** Minimize output by removing empty objects */
  minimize?: boolean;
  /** Schema id virtual */
  id?: boolean;
  /** Strict mode */
  strict?: boolean | 'throw';
  /** Strict for queries */
  strictQuery?: boolean | 'throw';
  /** toJSON options */
  toJSON?: ToObjectOptions;
  /** toObject options */
  toObject?: ToObjectOptions;
  /** Collection capped options */
  capped?: boolean | number | { size: number; max?: number };
  /** Read preference */
  read?: string;
  /** Write concern */
  writeConcern?: { w?: number | string; j?: boolean; wtimeout?: number };
  /** Sharding key */
  shardKey?: Record<string, 1 | -1>;
  /** Discriminator key */
  discriminatorKey?: string;
  /** Select populated fields by default */
  selectPopulatedPaths?: boolean;
  /** Collation */
  collation?: {
    locale: string;
    strength?: number;
    caseLevel?: boolean;
    caseFirst?: string;
    numericOrdering?: boolean;
    alternate?: string;
    maxVariable?: string;
    backwards?: boolean;
  };
  /** Overwrite models (for hot reloading) */
  overwriteModels?: boolean;
}

/**
 * toJSON/toObject options
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

// ============================================================================
// Middleware Types
// ============================================================================

/**
 * Middleware hook types
 */
export type MiddlewareHookType =
  | 'save'
  | 'validate'
  | 'remove'
  | 'deleteOne'
  | 'deleteMany'
  | 'updateOne'
  | 'updateMany'
  | 'findOneAndUpdate'
  | 'findOneAndDelete'
  | 'findOneAndReplace'
  | 'init'
  | 'insertMany'
  | 'aggregate';

/**
 * Query middleware types
 */
export type QueryMiddlewareType =
  | 'count'
  | 'countDocuments'
  | 'estimatedDocumentCount'
  | 'deleteMany'
  | 'deleteOne'
  | 'distinct'
  | 'find'
  | 'findOne'
  | 'findOneAndDelete'
  | 'findOneAndRemove'
  | 'findOneAndReplace'
  | 'findOneAndUpdate'
  | 'replaceOne'
  | 'updateMany'
  | 'updateOne'
  | 'validate';

/**
 * Pre middleware function
 */
export type PreMiddlewareFunction<T = unknown> = (
  this: T,
  next: (err?: Error) => void
) => void | Promise<void>;

/**
 * Post middleware function
 */
export type PostMiddlewareFunction<T = unknown> = (
  this: T,
  doc: T,
  next: (err?: Error) => void
) => void | Promise<void>;

/**
 * Middleware entry
 */
interface MiddlewareEntry {
  type: 'pre' | 'post';
  hook: MiddlewareHookType | QueryMiddlewareType;
  fn: PreMiddlewareFunction | PostMiddlewareFunction;
  options?: { document?: boolean; query?: boolean };
}

// ============================================================================
// Virtual Types
// ============================================================================

/**
 * Virtual getter/setter definition
 */
export interface VirtualDefinition {
  get?: (this: unknown) => unknown;
  set?: (this: unknown, value: unknown) => void;
  options?: VirtualOptions;
}

/**
 * Virtual options (for populate)
 */
export interface VirtualOptions {
  ref?: string;
  localField?: string;
  foreignField?: string;
  justOne?: boolean;
  count?: boolean;
  match?: Record<string, unknown>;
  options?: Record<string, unknown>;
}

/**
 * Virtual field instance
 */
export class Virtual {
  private _get?: (this: unknown) => unknown;
  private _set?: (this: unknown, value: unknown) => void;
  public options: VirtualOptions;

  constructor(options: VirtualOptions = {}) {
    this.options = options;
  }

  /**
   * Define getter for virtual
   */
  get(fn: (this: unknown) => unknown): this {
    this._get = fn;
    return this;
  }

  /**
   * Define setter for virtual
   */
  set(fn: (this: unknown, value: unknown) => void): this {
    this._set = fn;
    return this;
  }

  /**
   * Apply virtual getter to document
   */
  applyGetters(doc: Record<string, unknown>): unknown {
    if (this._get) {
      return this._get.call(doc);
    }
    return undefined;
  }

  /**
   * Apply virtual setter to document
   */
  applySetters(doc: Record<string, unknown>, value: unknown): void {
    if (this._set) {
      this._set.call(doc, value);
    }
  }

  /**
   * Check if virtual has getter
   */
  hasGetter(): boolean {
    return this._get !== undefined;
  }

  /**
   * Check if virtual has setter
   */
  hasSetter(): boolean {
    return this._set !== undefined;
  }
}

// ============================================================================
// Plugin Types
// ============================================================================

/**
 * Plugin function signature
 */
export type PluginFunction<T = unknown> = (schema: Schema<T>, options?: Record<string, unknown>) => void;

// ============================================================================
// Schema Path
// ============================================================================

/**
 * Represents a path in the schema
 */
export class SchemaPath {
  public path: string;
  public instance: string;
  public options: SchemaTypeDefinition;
  public validators: ValidatorDefinition[];
  public getters: Array<(value: unknown) => unknown>;
  public setters: Array<(value: unknown) => unknown>;
  public defaultValue?: unknown;
  public isRequired: boolean;

  constructor(path: string, type: SchemaType | SchemaTypeDefinition) {
    this.path = path;
    this.validators = [];
    this.getters = [];
    this.setters = [];
    this.isRequired = false;

    // Normalize type definition
    if (typeof type === 'object' && type !== null && 'type' in type) {
      this.options = type as SchemaTypeDefinition;
      this.instance = this.getTypeName(type.type);
    } else {
      this.options = { type: type as SchemaType };
      this.instance = this.getTypeName(type);
    }

    // Extract options
    this.extractOptions();
  }

  private getTypeName(type: unknown): string {
    if (type === String || type === 'String') return 'String';
    if (type === Number || type === 'Number') return 'Number';
    if (type === Boolean || type === 'Boolean') return 'Boolean';
    if (type === Date || type === 'Date') return 'Date';
    if (type === Buffer || type === 'Buffer') return 'Buffer';
    if (type === ObjectId || type === 'ObjectId') return 'ObjectId';
    if (type === Array || type === 'Array') return 'Array';
    if (type === Object || type === 'Mixed') return 'Mixed';
    if (type === Map || type === 'Map') return 'Map';
    if (type === 'Decimal128') return 'Decimal128';
    if (type === 'UUID') return 'UUID';
    if (type === 'BigInt') return 'BigInt';
    if (Array.isArray(type)) return 'Array';
    if (typeof type === 'object') return 'Embedded';
    return 'Mixed';
  }

  private extractOptions(): void {
    const opts = this.options;

    // Required
    if (opts.required) {
      this.isRequired = true;
      if (Array.isArray(opts.required)) {
        this.validators.push({
          validator: (v) => v != null,
          message: opts.required[1],
        });
      } else {
        this.validators.push({
          validator: (v) => v != null,
          message: `Path \`${this.path}\` is required.`,
        });
      }
    }

    // Default value
    if (opts.default !== undefined) {
      this.defaultValue = opts.default;
    }

    // Validators
    if (opts.validate) {
      if (typeof opts.validate === 'function') {
        this.validators.push({ validator: opts.validate });
      } else if (Array.isArray(opts.validate)) {
        this.validators.push(...opts.validate);
      } else {
        this.validators.push(opts.validate);
      }
    }

    // Enum validator
    if (opts.enum) {
      this.validators.push({
        validator: (v) => v == null || opts.enum!.includes(v),
        message: `\`{VALUE}\` is not a valid enum value for path \`${this.path}\`.`,
      });
    }

    // Min/Max validators
    if (opts.min !== undefined) {
      const [minVal, minMsg] = Array.isArray(opts.min) ? opts.min : [opts.min, undefined];
      this.validators.push({
        validator: (v) => v == null || (v as number) >= (minVal as number),
        message: minMsg || `Path \`${this.path}\` (${minVal}) is less than minimum allowed value.`,
      });
    }

    if (opts.max !== undefined) {
      const [maxVal, maxMsg] = Array.isArray(opts.max) ? opts.max : [opts.max, undefined];
      this.validators.push({
        validator: (v) => v == null || (v as number) <= (maxVal as number),
        message: maxMsg || `Path \`${this.path}\` (${maxVal}) is more than maximum allowed value.`,
      });
    }

    // String validators
    if (opts.minlength !== undefined) {
      const [minLen, minMsg] = Array.isArray(opts.minlength) ? opts.minlength : [opts.minlength, undefined];
      this.validators.push({
        validator: (v) => v == null || (v as string).length >= minLen,
        message: minMsg || `Path \`${this.path}\` is shorter than minimum allowed length (${minLen}).`,
      });
    }

    if (opts.maxlength !== undefined) {
      const [maxLen, maxMsg] = Array.isArray(opts.maxlength) ? opts.maxlength : [opts.maxlength, undefined];
      this.validators.push({
        validator: (v) => v == null || (v as string).length <= maxLen,
        message: maxMsg || `Path \`${this.path}\` is longer than maximum allowed length (${maxLen}).`,
      });
    }

    if (opts.match) {
      const [regex, matchMsg] = Array.isArray(opts.match) ? opts.match : [opts.match, undefined];
      this.validators.push({
        validator: (v) => v == null || regex.test(v as string),
        message: matchMsg || `Path \`${this.path}\` is invalid.`,
      });
    }

    // Getters
    if (opts.get) {
      this.getters.push(opts.get);
    }

    // Setters with transformations
    if (opts.set) {
      this.setters.push(opts.set);
    }
    if (opts.lowercase) {
      this.setters.push((v) => (typeof v === 'string' ? v.toLowerCase() : v));
    }
    if (opts.uppercase) {
      this.setters.push((v) => (typeof v === 'string' ? v.toUpperCase() : v));
    }
    if (opts.trim) {
      this.setters.push((v) => (typeof v === 'string' ? v.trim() : v));
    }
  }

  /**
   * Get default value for this path
   */
  getDefault(): unknown {
    if (typeof this.defaultValue === 'function') {
      return this.defaultValue();
    }
    // Deep clone objects to prevent shared references
    if (this.defaultValue && typeof this.defaultValue === 'object') {
      return JSON.parse(JSON.stringify(this.defaultValue));
    }
    return this.defaultValue;
  }

  /**
   * Apply setters to a value
   */
  applySetters(value: unknown): unknown {
    return this.setters.reduce((v, setter) => setter(v), value);
  }

  /**
   * Apply getters to a value
   */
  applyGetters(value: unknown): unknown {
    return this.getters.reduce((v, getter) => getter(v), value);
  }

  /**
   * Validate a value
   */
  async validate(value: unknown): Promise<{ valid: boolean; errors: string[] }> {
    const errors: string[] = [];

    for (const validator of this.validators) {
      try {
        const result = await validator.validator(value);
        if (!result) {
          const msg = typeof validator.message === 'function'
            ? validator.message({ value, path: this.path })
            : (validator.message || `Validation failed for path \`${this.path}\``);
          errors.push(msg.replace('{VALUE}', String(value)));
        }
      } catch (err) {
        errors.push(`Validation error for path \`${this.path}\`: ${err}`);
      }
    }

    return { valid: errors.length === 0, errors };
  }

  /**
   * Cast value to the correct type
   */
  cast(value: unknown): unknown {
    if (value == null) return value;

    switch (this.instance) {
      case 'String':
        return String(value);
      case 'Number':
        return Number(value);
      case 'Boolean':
        return Boolean(value);
      case 'Date':
        return value instanceof Date ? value : new Date(value as string | number);
      case 'ObjectId':
        return value instanceof ObjectId ? value : new ObjectId(String(value));
      case 'Buffer':
        return value instanceof Uint8Array ? value : new TextEncoder().encode(String(value));
      default:
        return value;
    }
  }
}

// ============================================================================
// Schema Class
// ============================================================================

/**
 * Mongoose-compatible Schema class
 */
export class Schema<T = unknown> {
  public paths: Map<string, SchemaPath> = new Map();
  public virtuals: Map<string, Virtual> = new Map();
  public methods: Map<string, Function> = new Map();
  public statics: Map<string, Function> = new Map();
  public options: SchemaOptions;
  public indexes: Array<{ fields: Record<string, 1 | -1 | 'text'>; options?: Record<string, unknown> }> = [];

  private middleware: MiddlewareEntry[] = [];
  private plugins: Array<{ fn: PluginFunction; options?: Record<string, unknown> }> = [];
  private childSchemas: Map<string, Schema> = new Map();

  constructor(definition?: SchemaDefinition, options?: SchemaOptions) {
    this.options = {
      _id: true,
      id: true,
      versionKey: '__v',
      strict: true,
      minimize: true,
      autoIndex: true,
      ...options,
    };

    // Add _id path if enabled
    if (this.options._id) {
      this.path('_id', { type: 'ObjectId', default: () => new ObjectId() });
    }

    // Add version key if enabled
    if (this.options.versionKey) {
      const versionKey = typeof this.options.versionKey === 'string' ? this.options.versionKey : '__v';
      this.path(versionKey, { type: Number, default: 0 });
    }

    // Add id virtual if enabled
    if (this.options.id) {
      this.virtual('id').get(function (this: unknown) {
        return (this as Record<string, unknown>)._id?.toString();
      });
    }

    // Process definition
    if (definition) {
      this.add(definition);
    }

    // Add timestamps if enabled
    if (this.options.timestamps) {
      const createdAt = typeof this.options.timestamps === 'object' && this.options.timestamps.createdAt
        ? (this.options.timestamps.createdAt === true ? 'createdAt' : this.options.timestamps.createdAt)
        : 'createdAt';
      const updatedAt = typeof this.options.timestamps === 'object' && this.options.timestamps.updatedAt
        ? (this.options.timestamps.updatedAt === true ? 'updatedAt' : this.options.timestamps.updatedAt)
        : 'updatedAt';

      if (createdAt) {
        this.path(createdAt, { type: Date, default: () => new Date() });
      }
      if (updatedAt) {
        this.path(updatedAt, { type: Date, default: () => new Date() });
      }
    }
  }

  /**
   * Add paths to the schema
   */
  add(definition: SchemaDefinition, prefix: string = ''): this {
    for (const [key, value] of Object.entries(definition)) {
      const fullPath = prefix ? `${prefix}.${key}` : key;

      // Handle nested schema
      if (value instanceof Schema) {
        this.childSchemas.set(fullPath, value);
        // Add nested paths with prefix
        for (const [nestedPath, nestedType] of value.paths) {
          this.path(`${fullPath}.${nestedPath}`, nestedType.options);
        }
      } else if (this.isSchemaTypeDefinition(value)) {
        this.path(fullPath, value);
      } else if (Array.isArray(value)) {
        // Array type
        this.path(fullPath, { type: value } as SchemaTypeDefinition);
      } else if (typeof value === 'object' && value !== null && !('type' in value)) {
        // Nested object - recurse
        this.add(value as SchemaDefinition, fullPath);
      } else {
        this.path(fullPath, value as SchemaType | SchemaTypeDefinition);
      }
    }
    return this;
  }

  private isSchemaTypeDefinition(value: unknown): value is SchemaTypeDefinition {
    return typeof value === 'object' && value !== null && 'type' in value;
  }

  /**
   * Define or get a path
   */
  path(name: string, type?: SchemaType | SchemaTypeDefinition | SchemaPath): SchemaPath | undefined {
    if (type === undefined) {
      return this.paths.get(name);
    }

    if (type instanceof SchemaPath) {
      this.paths.set(name, type);
    } else {
      this.paths.set(name, new SchemaPath(name, type));
    }

    return this.paths.get(name);
  }

  /**
   * Define a virtual field
   */
  virtual(name: string, options?: VirtualOptions): Virtual {
    const existing = this.virtuals.get(name);
    if (existing) {
      if (options) {
        Object.assign(existing.options, options);
      }
      return existing;
    }

    const virtual = new Virtual(options);
    this.virtuals.set(name, virtual);
    return virtual;
  }

  /**
   * Add instance method
   */
  method(name: string, fn: Function): this;
  method(methods: Record<string, Function>): this;
  method(nameOrMethods: string | Record<string, Function>, fn?: Function): this {
    if (typeof nameOrMethods === 'string' && fn) {
      this.methods.set(nameOrMethods, fn);
    } else if (typeof nameOrMethods === 'object') {
      for (const [name, method] of Object.entries(nameOrMethods)) {
        this.methods.set(name, method);
      }
    }
    return this;
  }

  /**
   * Add static method
   */
  static(name: string, fn: Function): this;
  static(statics: Record<string, Function>): this;
  static(nameOrStatics: string | Record<string, Function>, fn?: Function): this {
    if (typeof nameOrStatics === 'string' && fn) {
      this.statics.set(nameOrStatics, fn);
    } else if (typeof nameOrStatics === 'object') {
      for (const [name, method] of Object.entries(nameOrStatics)) {
        this.statics.set(name, method);
      }
    }
    return this;
  }

  /**
   * Add pre middleware
   */
  pre<HookType extends MiddlewareHookType | QueryMiddlewareType>(
    hook: HookType | HookType[],
    fn: PreMiddlewareFunction<T>,
    options?: { document?: boolean; query?: boolean }
  ): this {
    const hooks = Array.isArray(hook) ? hook : [hook];
    for (const h of hooks) {
      this.middleware.push({ type: 'pre', hook: h, fn: fn as PreMiddlewareFunction, options });
    }
    return this;
  }

  /**
   * Add post middleware
   */
  post<HookType extends MiddlewareHookType | QueryMiddlewareType>(
    hook: HookType | HookType[],
    fn: PostMiddlewareFunction<T>,
    options?: { document?: boolean; query?: boolean }
  ): this {
    const hooks = Array.isArray(hook) ? hook : [hook];
    for (const h of hooks) {
      this.middleware.push({ type: 'post', hook: h, fn: fn as PostMiddlewareFunction, options });
    }
    return this;
  }

  /**
   * Register a plugin
   */
  plugin(fn: PluginFunction<T>, options?: Record<string, unknown>): this {
    this.plugins.push({ fn: fn as PluginFunction, options });
    fn(this, options);
    return this;
  }

  /**
   * Get all plugins
   */
  getPlugins(): Array<{ fn: PluginFunction; options?: Record<string, unknown> }> {
    return this.plugins;
  }

  /**
   * Add index
   */
  index(fields: Record<string, 1 | -1 | 'text'>, options?: Record<string, unknown>): this {
    this.indexes.push({ fields, options });
    return this;
  }

  /**
   * Get pre middleware for a hook
   */
  getPreMiddleware(hook: MiddlewareHookType | QueryMiddlewareType): PreMiddlewareFunction[] {
    return this.middleware
      .filter((m) => m.type === 'pre' && m.hook === hook)
      .map((m) => m.fn as PreMiddlewareFunction);
  }

  /**
   * Get post middleware for a hook
   */
  getPostMiddleware(hook: MiddlewareHookType | QueryMiddlewareType): PostMiddlewareFunction[] {
    return this.middleware
      .filter((m) => m.type === 'post' && m.hook === hook)
      .map((m) => m.fn as PostMiddlewareFunction);
  }

  /**
   * Run pre middleware chain
   */
  async runPreMiddleware<DocType>(hook: MiddlewareHookType | QueryMiddlewareType, context: DocType): Promise<void> {
    const middleware = this.getPreMiddleware(hook);

    for (const fn of middleware) {
      await new Promise<void>((resolve, reject) => {
        fn.call(context, (err?: Error) => {
          if (err) reject(err);
          else resolve();
        });
      });
    }
  }

  /**
   * Run post middleware chain
   */
  async runPostMiddleware<DocType>(
    hook: MiddlewareHookType | QueryMiddlewareType,
    context: DocType,
    doc: DocType
  ): Promise<void> {
    const middleware = this.getPostMiddleware(hook);

    for (const fn of middleware) {
      await new Promise<void>((resolve, reject) => {
        fn.call(context, doc, (err?: Error) => {
          if (err) reject(err);
          else resolve();
        });
      });
    }
  }

  /**
   * Get all path names
   */
  pathNames(): string[] {
    return Array.from(this.paths.keys());
  }

  /**
   * Get all paths that are required
   */
  requiredPaths(): string[] {
    return Array.from(this.paths.entries())
      .filter(([, path]) => path.isRequired)
      .map(([name]) => name);
  }

  /**
   * Check if a path exists
   */
  pathType(path: string): 'real' | 'virtual' | 'nested' | 'adhocOrUndefined' {
    if (this.paths.has(path)) return 'real';
    if (this.virtuals.has(path)) return 'virtual';

    // Check for nested paths
    for (const p of this.paths.keys()) {
      if (p.startsWith(path + '.')) return 'nested';
    }

    return 'adhocOrUndefined';
  }

  /**
   * Clone the schema
   */
  clone(): Schema<T> {
    const cloned = new Schema<T>(undefined, { ...this.options });

    // Copy paths
    for (const [name, path] of this.paths) {
      cloned.paths.set(name, path);
    }

    // Copy virtuals
    for (const [name, virtual] of this.virtuals) {
      cloned.virtuals.set(name, virtual);
    }

    // Copy methods
    for (const [name, method] of this.methods) {
      cloned.methods.set(name, method);
    }

    // Copy statics
    for (const [name, stat] of this.statics) {
      cloned.statics.set(name, stat);
    }

    // Copy middleware
    cloned.middleware = [...this.middleware];

    // Copy indexes
    cloned.indexes = [...this.indexes];

    // Copy plugins
    cloned.plugins = [...this.plugins];

    return cloned;
  }

  /**
   * Pick specific paths from the schema
   */
  pick(paths: string[], options?: SchemaOptions): Schema {
    const picked = new Schema(undefined, { ...this.options, ...options });

    for (const path of paths) {
      const schemaPath = this.paths.get(path);
      if (schemaPath) {
        picked.paths.set(path, schemaPath);
      }
    }

    return picked;
  }

  /**
   * Omit specific paths from the schema
   */
  omit(paths: string[], options?: SchemaOptions): Schema {
    const omitted = this.clone();

    for (const path of paths) {
      omitted.paths.delete(path);
    }

    if (options) {
      Object.assign(omitted.options, options);
    }

    return omitted;
  }

  /**
   * Get JSON representation of schema
   */
  toJSON(): Record<string, unknown> {
    const paths: Record<string, unknown> = {};
    for (const [name, path] of this.paths) {
      paths[name] = {
        instance: path.instance,
        options: path.options,
        isRequired: path.isRequired,
      };
    }

    return {
      paths,
      virtuals: Array.from(this.virtuals.keys()),
      methods: Array.from(this.methods.keys()),
      statics: Array.from(this.statics.keys()),
      indexes: this.indexes,
      options: this.options,
    };
  }
}

// ============================================================================
// Type Exports
// ============================================================================

export type {
  MiddlewareEntry,
};
