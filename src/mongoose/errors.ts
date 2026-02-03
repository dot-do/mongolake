/**
 * MongoLake Mongoose Errors
 *
 * Provides Mongoose-compatible error classes for:
 * - Validation errors
 * - Cast errors
 * - Document not found
 * - Duplicate key
 * - Disconnected
 * - Version error
 * - Parallel save error
 */

// ============================================================================
// Base Error
// ============================================================================

/**
 * Base Mongoose error
 */
export class MongooseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'MongooseError';
  }
}

// ============================================================================
// Validation Errors
// ============================================================================

/**
 * Validation error for document validation failures
 */
export class ValidationError extends MongooseError {
  public errors: Record<string, ValidatorError>;
  public _message: string;

  constructor(instance?: { constructor: { modelName?: string } }) {
    const modelName = instance?.constructor?.modelName || 'Document';
    super(`${modelName} validation failed`);
    this.name = 'ValidationError';
    this._message = `${modelName} validation failed`;
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

  toString(): string {
    return this.message;
  }
}

/**
 * Individual validator error
 */
export class ValidatorError extends MongooseError {
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
    message?: string;
    type?: string;
    kind?: string;
    path: string;
    value?: unknown;
    reason?: Error;
  }) {
    const message = options.message || `Validation failed`;
    super(message);
    this.name = 'ValidatorError';
    this.kind = options.kind || options.type || 'user defined';
    this.path = options.path;
    this.value = options.value;
    this.reason = options.reason;
    this.properties = {
      message,
      type: this.kind,
      path: options.path,
      value: options.value,
      reason: options.reason,
    };
  }
}

// ============================================================================
// Cast Error
// ============================================================================

/**
 * Cast error for type coercion failures
 */
export class CastError extends MongooseError {
  public kind: string;
  public value: unknown;
  public path: string;
  public reason?: Error;
  public model?: string;
  public stringValue: string;
  public valueType: string;

  constructor(type: string, value: unknown, path: string, reason?: Error, _schemaType?: string) {
    const stringValue = value === null ? 'null' : value === undefined ? 'undefined' : String(value);
    super(`Cast to ${type} failed for value "${stringValue}" (type ${typeof value}) at path "${path}"`);
    this.name = 'CastError';
    this.kind = type;
    this.value = value;
    this.path = path;
    this.reason = reason;
    this.stringValue = stringValue;
    this.valueType = typeof value;
  }

  /**
   * Set the model name for better error messages
   */
  setModel(model: string): void {
    this.model = model;
  }
}

// ============================================================================
// Document Errors
// ============================================================================

/**
 * Document not found error
 */
export class DocumentNotFoundError extends MongooseError {
  public filter: Record<string, unknown>;
  public query: Record<string, unknown>;

  constructor(filter: Record<string, unknown>) {
    super('No document found for query');
    this.name = 'DocumentNotFoundError';
    this.filter = filter;
    this.query = filter;
  }
}

/**
 * Version error for optimistic concurrency conflicts
 */
export class VersionError extends MongooseError {
  public version: number;
  public modifiedPaths: string[];

  constructor(doc: { _id?: unknown; __v?: number }, currentVersion: number, modifiedPaths: string[]) {
    super(
      `No matching document found for id "${doc._id}" version ${currentVersion} ` +
        `modifiedPaths "${modifiedPaths.join(', ')}"`
    );
    this.name = 'VersionError';
    this.version = currentVersion;
    this.modifiedPaths = modifiedPaths;
  }
}

/**
 * Parallel save error
 */
export class ParallelSaveError extends MongooseError {
  constructor(doc: { _id?: unknown }) {
    super(`Can't save() the same doc multiple times in parallel. Document: ${doc._id}`);
    this.name = 'ParallelSaveError';
  }
}

// ============================================================================
// Connection Errors
// ============================================================================

/**
 * Disconnected error
 */
export class DisconnectedError extends MongooseError {
  constructor(connectionString?: string) {
    super(connectionString ? `Connection to ${connectionString} was lost` : 'Lost connection to database');
    this.name = 'DisconnectedError';
  }
}

/**
 * Missing schema error
 */
export class MissingSchemaError extends MongooseError {
  constructor(name: string) {
    super(`Schema hasn't been registered for model "${name}".`);
    this.name = 'MissingSchemaError';
  }
}

// ============================================================================
// Duplicate Key Error
// ============================================================================

/**
 * Duplicate key error (MongoDB E11000)
 */
export class DuplicateKeyError extends MongooseError {
  public code: number = 11000;
  public keyPattern: Record<string, unknown>;
  public keyValue: Record<string, unknown>;

  constructor(keyPattern: Record<string, unknown>, keyValue: Record<string, unknown>) {
    const keys = Object.keys(keyPattern).join(', ');
    super(`E11000 duplicate key error: duplicate key in ${keys}`);
    this.name = 'MongoServerError';
    this.keyPattern = keyPattern;
    this.keyValue = keyValue;
  }
}

// ============================================================================
// Strict Mode Error
// ============================================================================

/**
 * Strict mode violation error
 */
export class StrictModeError extends MongooseError {
  public path: string;
  public isImmutableError: boolean;

  constructor(path: string, msg?: string, immutable?: boolean) {
    super(msg || `Field \`${path}\` is not in schema and strict mode is set to throw.`);
    this.name = 'StrictModeError';
    this.path = path;
    this.isImmutableError = immutable || false;
  }
}

// ============================================================================
// Divergent Array Error
// ============================================================================

/**
 * Divergent array error
 */
export class DivergentArrayError extends MongooseError {
  constructor(paths: string[]) {
    super(`For your own good, Mongoose does not know what to do with the ${paths.join(', ')} path(s).`);
    this.name = 'DivergentArrayError';
  }
}

// ============================================================================
// Object Expected Error
// ============================================================================

/**
 * Object expected error
 */
export class ObjectExpectedError extends MongooseError {
  public path: string;

  constructor(type: string, path: string) {
    super(`Tried to set nested object field \`${path}\` to primitive value \`${type}\`.`);
    this.name = 'ObjectExpectedError';
    this.path = path;
  }
}

// ============================================================================
// Object Parameter Error
// ============================================================================

/**
 * Object parameter error
 */
export class ObjectParameterError extends MongooseError {
  constructor(value: unknown, parameterName: string, method: string) {
    super(
      `Parameter "${parameterName}" to ${method}() must be an object, ` +
        `got "${value}" (type ${typeof value})`
    );
    this.name = 'ObjectParameterError';
  }
}

// ============================================================================
// Overwrite Model Error
// ============================================================================

/**
 * Overwrite model error
 */
export class OverwriteModelError extends MongooseError {
  constructor(name: string) {
    super(
      `Cannot overwrite \`${name}\` model once compiled. ` +
        `Use mongoose.deleteModel('${name}') to remove it first.`
    );
    this.name = 'OverwriteModelError';
  }
}

// ============================================================================
// Error Messages Helper
// ============================================================================

/**
 * Error message templates
 */
export const messages = {
  general: {
    default: 'Validator failed for path `{PATH}` with value `{VALUE}`',
    required: 'Path `{PATH}` is required.',
    cast: 'Cast to {KIND} failed for value "{VALUE}" at path "{PATH}"',
  },
  Number: {
    min: 'Path `{PATH}` ({VALUE}) is less than minimum allowed value ({MIN}).',
    max: 'Path `{PATH}` ({VALUE}) is more than maximum allowed value ({MAX}).',
  },
  Date: {
    min: 'Path `{PATH}` ({VALUE}) is before minimum allowed value ({MIN}).',
    max: 'Path `{PATH}` ({VALUE}) is after maximum allowed value ({MAX}).',
  },
  String: {
    enum: '`{VALUE}` is not a valid enum value for path `{PATH}`.',
    match: 'Path `{PATH}` is invalid ({VALUE}).',
    minlength: 'Path `{PATH}` (`{VALUE}`) is shorter than the minimum allowed length ({MINLENGTH}).',
    maxlength: 'Path `{PATH}` (`{VALUE}`) is longer than the maximum allowed length ({MAXLENGTH}).',
  },
};

/**
 * Format error message with values
 */
export function formatMessage(template: string, values: Record<string, unknown>): string {
  let result = template;
  for (const [key, value] of Object.entries(values)) {
    result = result.replace(new RegExp(`\\{${key}\\}`, 'g'), String(value));
  }
  return result;
}
