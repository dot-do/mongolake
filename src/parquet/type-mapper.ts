/**
 * Parquet Type Mapper
 *
 * Maps MongoDB/BSON types to Parquet types with support for:
 * - All BSON type mappings
 * - Nested documents (struct) and arrays (list)
 * - Type promotion rules
 * - Null/optional field handling
 * - Extensible type registry for custom type handlers
 * - Optimized type inference
 *
 * @module parquet/type-mapper
 */

// ============================================================================
// BSON Type Constants
// ============================================================================

/**
 * MongoDB BSON type identifiers.
 * These match the MongoDB $type operator values.
 *
 * @see https://www.mongodb.com/docs/manual/reference/bson-types/
 *
 * @example
 * ```typescript
 * import { BSONType } from './type-mapper';
 *
 * const type = BSONType.STRING; // 'string'
 * ```
 */
export const BSONType = {
  /** IEEE 754 double-precision floating point */
  DOUBLE: 'double',
  /** UTF-8 encoded string */
  STRING: 'string',
  /** Embedded document (nested object) */
  OBJECT: 'object',
  /** Array of values */
  ARRAY: 'array',
  /** Binary data */
  BINARY: 'binData',
  /** MongoDB ObjectId (12-byte identifier) */
  OBJECT_ID: 'objectId',
  /** Boolean value */
  BOOLEAN: 'bool',
  /** UTC datetime (milliseconds since epoch) */
  DATE: 'date',
  /** Null value */
  NULL: 'null',
  /** Regular expression */
  REGEX: 'regex',
  /** 32-bit signed integer */
  INT32: 'int',
  /** MongoDB internal timestamp (for replication) */
  TIMESTAMP: 'timestamp',
  /** 64-bit signed integer */
  INT64: 'long',
  /** 128-bit decimal floating point */
  DECIMAL128: 'decimal',
  /** Minimum key (internal sorting) */
  MIN_KEY: 'minKey',
  /** Maximum key (internal sorting) */
  MAX_KEY: 'maxKey',
} as const;

export type BSONTypeValue = (typeof BSONType)[keyof typeof BSONType];

// ============================================================================
// Parquet Type Constants
// ============================================================================

/**
 * Parquet physical types (primitive types stored on disk).
 *
 * These are the fundamental storage types in Parquet format.
 *
 * @see https://parquet.apache.org/docs/file-format/types/
 *
 * @example
 * ```typescript
 * import { ParquetPhysicalType } from './type-mapper';
 *
 * const intType = ParquetPhysicalType.INT32; // 'INT32'
 * ```
 */
export const ParquetPhysicalType = {
  /** 1-bit boolean */
  BOOLEAN: 'BOOLEAN',
  /** 32-bit signed integer */
  INT32: 'INT32',
  /** 64-bit signed integer */
  INT64: 'INT64',
  /** 96-bit signed integer (deprecated, legacy timestamp) */
  INT96: 'INT96',
  /** IEEE 754 single-precision floating point */
  FLOAT: 'FLOAT',
  /** IEEE 754 double-precision floating point */
  DOUBLE: 'DOUBLE',
  /** Variable-length byte array */
  BYTE_ARRAY: 'BYTE_ARRAY',
  /** Fixed-length byte array */
  FIXED_LEN_BYTE_ARRAY: 'FIXED_LEN_BYTE_ARRAY',
} as const;

export type ParquetPhysicalTypeValue =
  (typeof ParquetPhysicalType)[keyof typeof ParquetPhysicalType];

/**
 * Parquet logical types (semantic interpretation of physical types).
 *
 * Logical types provide semantic meaning to the underlying physical types.
 *
 * @see https://parquet.apache.org/docs/file-format/types/
 *
 * @example
 * ```typescript
 * import { ParquetLogicalType } from './type-mapper';
 *
 * const stringType = ParquetLogicalType.STRING; // 'STRING'
 * ```
 */
export const ParquetLogicalType = {
  /** UTF-8 encoded string */
  STRING: 'STRING',
  /** Key-value map */
  MAP: 'MAP',
  /** Repeated values (array) */
  LIST: 'LIST',
  /** Enumeration (fixed set of values) */
  ENUM: 'ENUM',
  /** Arbitrary precision decimal */
  DECIMAL: 'DECIMAL',
  /** Date without time */
  DATE: 'DATE',
  /** Time of day in milliseconds */
  TIME_MILLIS: 'TIME_MILLIS',
  /** Time of day in microseconds */
  TIME_MICROS: 'TIME_MICROS',
  /** Timestamp in milliseconds since epoch */
  TIMESTAMP_MILLIS: 'TIMESTAMP_MILLIS',
  /** Timestamp in microseconds since epoch */
  TIMESTAMP_MICROS: 'TIMESTAMP_MICROS',
  /** 8-bit unsigned integer */
  UINT_8: 'UINT_8',
  /** 16-bit unsigned integer */
  UINT_16: 'UINT_16',
  /** 32-bit unsigned integer */
  UINT_32: 'UINT_32',
  /** 64-bit unsigned integer */
  UINT_64: 'UINT_64',
  /** 8-bit signed integer */
  INT_8: 'INT_8',
  /** 16-bit signed integer */
  INT_16: 'INT_16',
  /** 32-bit signed integer */
  INT_32: 'INT_32',
  /** 64-bit signed integer */
  INT_64: 'INT_64',
  /** JSON-encoded data */
  JSON: 'JSON',
  /** BSON-encoded data */
  BSON: 'BSON',
  /** UUID (128-bit identifier) */
  UUID: 'UUID',
} as const;

export type ParquetLogicalTypeValue =
  (typeof ParquetLogicalType)[keyof typeof ParquetLogicalType];

// ============================================================================
// Type Mapping Interfaces
// ============================================================================

/**
 * Represents a mapping from a BSON type to a Parquet type
 */
export interface TypeMapping {
  /** The Parquet physical type for storage */
  physicalType: ParquetPhysicalTypeValue;
  /** Optional logical type for semantic interpretation */
  logicalType?: ParquetLogicalTypeValue;
  /** Fixed length for FIXED_LEN_BYTE_ARRAY types */
  typeLength?: number;
  /** Precision for DECIMAL types */
  precision?: number;
  /** Scale for DECIMAL types */
  scale?: number;
  /** Whether this type should be stored as variant encoding */
  isVariant?: boolean;
}

/**
 * Schema field definition for conversion
 */
export interface SchemaField {
  /** Field name */
  name: string;
  /** BSON type of the field */
  type: BSONTypeValue;
  /** Whether the field is optional (nullable) */
  optional?: boolean;
  /** Child fields for OBJECT types */
  children?: SchemaField[];
  /** Element type for ARRAY types */
  elementType?: BSONTypeValue;
  /** Whether array elements are optional */
  elementOptional?: boolean;
  /** Children for array elements that are objects */
  elementChildren?: SchemaField[];
  /** Nested element type for arrays of arrays */
  nestedElementType?: BSONTypeValue;
}

/**
 * Parquet schema field definition
 */
export interface ParquetSchemaField {
  /** Field name */
  name: string;
  /** Physical storage type */
  physicalType: ParquetPhysicalTypeValue;
  /** Logical interpretation type */
  logicalType?: ParquetLogicalTypeValue;
  /** Whether the field is optional */
  optional?: boolean;
  /** Fixed byte length for FIXED_LEN_BYTE_ARRAY */
  typeLength?: number;
  /** Whether this is a struct (nested object) */
  isStruct?: boolean;
  /** Child fields for structs */
  children?: ParquetSchemaField[];
  /** Whether this is a list (array) */
  isList?: boolean;
  /** Element type for lists */
  elementType?: ParquetSchemaField;
  /** Whether stored as variant */
  isVariant?: boolean;
  /** Precision for decimal types */
  precision?: number;
  /** Scale for decimal types */
  scale?: number;
}

/**
 * Complete Parquet schema
 */
export interface ParquetSchema {
  /** Root fields of the schema */
  fields: ParquetSchemaField[];
}

/**
 * Result of mapping a value to Parquet format
 */
export interface MappedValue {
  /** The mapped value (may be transformed) */
  value: unknown;
  /** The detected BSON type */
  bsonType: BSONTypeValue;
  /** Whether the value is null */
  isNull: boolean;
  /** Whether to use variant encoding */
  useVariant?: boolean;
}

// ============================================================================
// Extensible Type Registry
// ============================================================================

/**
 * Custom type handler for extending the type mapper.
 *
 * Allows registration of custom BSON-to-Parquet type mappings
 * and value transformations.
 *
 * @example
 * ```typescript
 * const customHandler: TypeHandler = {
 *   name: 'GeoPoint',
 *   detect: (value) => {
 *     return typeof value === 'object' &&
 *       value !== null &&
 *       'lat' in value &&
 *       'lng' in value;
 *   },
 *   getMapping: () => ({
 *     physicalType: ParquetPhysicalType.BYTE_ARRAY,
 *     logicalType: ParquetLogicalType.JSON,
 *   }),
 *   transformValue: (value) => JSON.stringify(value),
 * };
 *
 * typeRegistry.register(customHandler);
 * ```
 */
export interface TypeHandler {
  /** Unique name for this type handler */
  readonly name: string;

  /** Priority for type detection (higher = checked first, default: 0) */
  readonly priority?: number;

  /**
   * Detect if this handler should process the given value.
   * @param value - The value to check
   * @returns True if this handler should process the value
   */
  detect(value: unknown): boolean;

  /**
   * Get the Parquet type mapping for this type.
   * @param value - The value being mapped (for dynamic mappings)
   * @returns The type mapping configuration
   */
  getMapping(value: unknown): TypeMapping;

  /**
   * Transform the value for Parquet storage.
   * @param value - The original value
   * @returns The transformed value
   */
  transformValue(value: unknown): unknown;

  /**
   * Get the BSON type identifier for this handler.
   * @returns The BSON type string
   */
  getBSONType(): BSONTypeValue;
}

/**
 * Registry for custom type handlers.
 *
 * Provides an extensible mechanism for adding custom BSON-to-Parquet
 * type mappings without modifying the core type mapper.
 *
 * @example
 * ```typescript
 * import { typeRegistry, TypeHandler } from './type-mapper';
 *
 * // Register a custom type handler
 * const handler: TypeHandler = { ... };
 * typeRegistry.register(handler);
 *
 * // Check if a value matches any custom handler
 * const match = typeRegistry.findHandler(myValue);
 * ```
 */
export interface TypeRegistry {
  /**
   * Register a custom type handler.
   * @param handler - The type handler to register
   * @throws Error if a handler with the same name already exists
   */
  register(handler: TypeHandler): void;

  /**
   * Unregister a type handler by name.
   * @param name - The handler name to remove
   * @returns True if the handler was removed
   */
  unregister(name: string): boolean;

  /**
   * Find the first matching handler for a value.
   * Handlers are checked in priority order (highest first).
   * @param value - The value to check
   * @returns The matching handler, or undefined if none match
   */
  findHandler(value: unknown): TypeHandler | undefined;

  /**
   * Get all registered handlers.
   * @returns Array of all registered handlers
   */
  getHandlers(): readonly TypeHandler[];

  /**
   * Check if a handler with the given name is registered.
   * @param name - The handler name to check
   * @returns True if the handler exists
   */
  has(name: string): boolean;

  /**
   * Clear all registered handlers.
   */
  clear(): void;
}

/**
 * Implementation of the type registry.
 * @internal
 */
class TypeRegistryImpl implements TypeRegistry {
  private handlers: Map<string, TypeHandler> = new Map();
  private sortedHandlers: TypeHandler[] = [];

  register(handler: TypeHandler): void {
    if (this.handlers.has(handler.name)) {
      throw new Error(`Type handler '${handler.name}' is already registered`);
    }
    this.handlers.set(handler.name, handler);
    this.rebuildSortedHandlers();
  }

  unregister(name: string): boolean {
    const removed = this.handlers.delete(name);
    if (removed) {
      this.rebuildSortedHandlers();
    }
    return removed;
  }

  findHandler(value: unknown): TypeHandler | undefined {
    for (const handler of this.sortedHandlers) {
      if (handler.detect(value)) {
        return handler;
      }
    }
    return undefined;
  }

  getHandlers(): readonly TypeHandler[] {
    return this.sortedHandlers;
  }

  has(name: string): boolean {
    return this.handlers.has(name);
  }

  clear(): void {
    this.handlers.clear();
    this.sortedHandlers = [];
  }

  private rebuildSortedHandlers(): void {
    this.sortedHandlers = Array.from(this.handlers.values()).sort(
      (a, b) => (b.priority ?? 0) - (a.priority ?? 0)
    );
  }
}

/**
 * Global type registry instance.
 *
 * Use this to register custom type handlers that extend the
 * default BSON-to-Parquet type mappings.
 *
 * @example
 * ```typescript
 * import { typeRegistry } from './type-mapper';
 *
 * typeRegistry.register({
 *   name: 'CustomDate',
 *   priority: 10,
 *   detect: (v) => v instanceof CustomDate,
 *   getMapping: () => ({
 *     physicalType: 'INT64',
 *     logicalType: 'TIMESTAMP_MICROS',
 *   }),
 *   transformValue: (v) => (v as CustomDate).toMicros(),
 *   getBSONType: () => 'date',
 * });
 * ```
 */
export const typeRegistry: TypeRegistry = new TypeRegistryImpl();

// ============================================================================
// BSON to Parquet Mapping
// ============================================================================

/**
 * Static mapping table from BSON types to Parquet types.
 * This defines the default mapping for each BSON type.
 */
const BSON_TO_PARQUET_MAP: Record<BSONTypeValue, TypeMapping> = {
  [BSONType.DOUBLE]: {
    physicalType: ParquetPhysicalType.DOUBLE,
  },
  [BSONType.STRING]: {
    physicalType: ParquetPhysicalType.BYTE_ARRAY,
    logicalType: ParquetLogicalType.STRING,
  },
  [BSONType.OBJECT]: {
    physicalType: ParquetPhysicalType.BYTE_ARRAY,
    isVariant: true,
  },
  [BSONType.ARRAY]: {
    physicalType: ParquetPhysicalType.BYTE_ARRAY,
    isVariant: true,
  },
  [BSONType.BINARY]: {
    physicalType: ParquetPhysicalType.BYTE_ARRAY,
  },
  [BSONType.OBJECT_ID]: {
    physicalType: ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY,
    typeLength: 12,
  },
  [BSONType.BOOLEAN]: {
    physicalType: ParquetPhysicalType.BOOLEAN,
  },
  [BSONType.DATE]: {
    physicalType: ParquetPhysicalType.INT64,
    logicalType: ParquetLogicalType.TIMESTAMP_MILLIS,
  },
  [BSONType.NULL]: {
    physicalType: ParquetPhysicalType.BYTE_ARRAY,
    isVariant: true,
  },
  [BSONType.REGEX]: {
    physicalType: ParquetPhysicalType.BYTE_ARRAY,
    isVariant: true,
  },
  [BSONType.INT32]: {
    physicalType: ParquetPhysicalType.INT32,
    logicalType: ParquetLogicalType.INT_32,
  },
  [BSONType.TIMESTAMP]: {
    physicalType: ParquetPhysicalType.INT64,
    logicalType: ParquetLogicalType.TIMESTAMP_MILLIS,
  },
  [BSONType.INT64]: {
    physicalType: ParquetPhysicalType.INT64,
    logicalType: ParquetLogicalType.INT_64,
  },
  [BSONType.DECIMAL128]: {
    physicalType: ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY,
    logicalType: ParquetLogicalType.DECIMAL,
    typeLength: 16,
    precision: 34,
    scale: 0,
  },
  [BSONType.MIN_KEY]: {
    physicalType: ParquetPhysicalType.BYTE_ARRAY,
    isVariant: true,
  },
  [BSONType.MAX_KEY]: {
    physicalType: ParquetPhysicalType.BYTE_ARRAY,
    isVariant: true,
  },
};

/**
 * Map a BSON type to its corresponding Parquet type mapping.
 *
 * Uses the built-in mapping table for standard BSON types.
 * For custom types, use the type registry to register handlers.
 *
 * @param bsonType - The BSON type to map
 * @returns The Parquet type mapping
 *
 * @example
 * ```typescript
 * const mapping = bsonToParquet(BSONType.STRING);
 * // { physicalType: 'BYTE_ARRAY', logicalType: 'STRING' }
 * ```
 *
 * @see {@link typeRegistry} for custom type mappings
 */
export function bsonToParquet(bsonType: BSONTypeValue): TypeMapping {
  const mapping = BSON_TO_PARQUET_MAP[bsonType];

  if (!mapping) {
    // Unknown type - use variant encoding
    return {
      physicalType: ParquetPhysicalType.BYTE_ARRAY,
      isVariant: true,
    };
  }

  return { ...mapping };
}

/**
 * Map a value to its Parquet type mapping.
 *
 * This function first checks registered custom type handlers,
 * then falls back to the default BSON type inference and mapping.
 *
 * @param value - The value to map
 * @returns The Parquet type mapping
 *
 * @example
 * ```typescript
 * const mapping = valueToParquetMapping(new Date());
 * // { physicalType: 'INT64', logicalType: 'TIMESTAMP_MILLIS' }
 * ```
 */
export function valueToParquetMapping(value: unknown): TypeMapping {
  // Check custom type handlers first
  const handler = typeRegistry.findHandler(value);
  if (handler) {
    return handler.getMapping(value);
  }

  // Fall back to default BSON type inference
  const bsonType = inferBSONType(value);
  return bsonToParquet(bsonType);
}

// ============================================================================
// BSON Type Inference
// ============================================================================

/** Cached Int32 range constants for performance */
const INT32_MIN = -2147483648;
const INT32_MAX = 2147483647;

/**
 * Infer the BSON type from a JavaScript value.
 *
 * This function uses an optimized type detection path for common types,
 * and supports custom type handlers via the type registry.
 *
 * @param value - The value to inspect
 * @returns The inferred BSON type
 *
 * @example
 * ```typescript
 * inferBSONType('hello');     // 'string'
 * inferBSONType(42);          // 'int'
 * inferBSONType(3.14);        // 'double'
 * inferBSONType(new Date());  // 'date'
 * inferBSONType([1, 2, 3]);   // 'array'
 * ```
 *
 * @remarks
 * Type detection order (optimized for common cases):
 * 1. Null/undefined (fast path)
 * 2. Primitives: string, boolean, bigint, number
 * 3. Built-in objects: Date, Uint8Array, RegExp, Array
 * 4. Custom type handlers (from registry)
 * 5. MongoDB types: ObjectId, Decimal128, Binary
 * 6. Default: object
 */
export function inferBSONType(value: unknown): BSONTypeValue {
  // Fast path: null and undefined (most common check)
  if (value === null || value === undefined) {
    return BSONType.NULL;
  }

  // Fast path: primitives (use typeof for performance)
  const valueType = typeof value;

  if (valueType === 'string') {
    return BSONType.STRING;
  }

  if (valueType === 'boolean') {
    return BSONType.BOOLEAN;
  }

  if (valueType === 'bigint') {
    return BSONType.INT64;
  }

  if (valueType === 'number') {
    return inferNumberType(value as number);
  }

  // Objects: check built-in types first (common cases)
  if (value instanceof Date) {
    return BSONType.DATE;
  }

  if (value instanceof Uint8Array || value instanceof ArrayBuffer) {
    return BSONType.BINARY;
  }

  // Node.js Buffer check (conditional)
  if (typeof Buffer !== 'undefined' && Buffer.isBuffer?.(value)) {
    return BSONType.BINARY;
  }

  if (value instanceof RegExp) {
    return BSONType.REGEX;
  }

  // Array check before object
  if (Array.isArray(value)) {
    return BSONType.ARRAY;
  }

  // Object types: check custom handlers first
  if (valueType === 'object') {
    // Check custom type handlers (allows extension)
    const handler = typeRegistry.findHandler(value);
    if (handler) {
      return handler.getBSONType();
    }

    // Check MongoDB-specific object types
    if (isObjectIdLike(value)) {
      return BSONType.OBJECT_ID;
    }

    if (isDecimal128Like(value)) {
      return BSONType.DECIMAL128;
    }

    if (isBinaryLike(value)) {
      return BSONType.BINARY;
    }

    return BSONType.OBJECT;
  }

  // Fallback
  return BSONType.NULL;
}

/**
 * Infer the specific numeric type for a number value.
 *
 * Optimized for performance with early returns and minimal branching.
 *
 * @param value - The number to classify
 * @returns BSONType.DOUBLE, BSONType.INT32, or BSONType.INT64
 * @internal
 */
function inferNumberType(value: number): BSONTypeValue {
  // Fast path: special float values (NaN, Infinity, -Infinity)
  // Also handles -0 which should be DOUBLE
  if (!Number.isFinite(value) || Object.is(value, -0)) {
    return BSONType.DOUBLE;
  }

  // Check if integer
  if (!Number.isInteger(value)) {
    return BSONType.DOUBLE;
  }

  // Check int32 range (using cached constants)
  if (value >= INT32_MIN && value <= INT32_MAX) {
    return BSONType.INT32;
  }

  return BSONType.INT64;
}

/**
 * Check if a value looks like a MongoDB ObjectId.
 *
 * @param value - The value to check
 * @returns True if the value appears to be an ObjectId
 * @internal
 */
function isObjectIdLike(value: unknown): boolean {
  if (typeof value !== 'object' || value === null) {
    return false;
  }

  const proto = Object.getPrototypeOf(value);
  if (proto && proto.constructor && proto.constructor.name === 'ObjectId') {
    return true;
  }

  // Check for ObjectId-like shape (has toHexString returning 24 hex chars)
  const obj = value as Record<string, unknown>;
  if (
    typeof obj.toString === 'function' &&
    typeof obj.toHexString === 'function'
  ) {
    const str = (obj.toHexString as () => string)();
    return typeof str === 'string' && /^[0-9a-fA-F]{24}$/.test(str);
  }

  return false;
}

/**
 * Check if a value looks like a MongoDB Decimal128.
 *
 * Detection methods:
 * 1. Check for `_bsontype === 'Decimal128'` property
 * 2. Check for `toJSON()` returning `{ $numberDecimal: string }`
 *
 * @param value - The value to check
 * @returns True if the value appears to be a Decimal128
 * @internal
 */
function isDecimal128Like(value: unknown): boolean {
  if (typeof value !== 'object' || value === null) {
    return false;
  }

  const obj = value as Record<string, unknown>;

  // Fast path: check for _bsontype property
  if (obj._bsontype === 'Decimal128') {
    return true;
  }

  // Check for toJSON returning $numberDecimal
  if (typeof obj.toJSON === 'function' && typeof obj.toString === 'function') {
    try {
      const json = (obj.toJSON as () => unknown)();
      if (typeof json === 'object' && json !== null && '$numberDecimal' in json) {
        return true;
      }
    } catch {
      // Not a Decimal128
    }
  }

  return false;
}

/**
 * Check if a value looks like a MongoDB Binary.
 *
 * Detection method: Check for `sub_type` (number) and `buffer` properties.
 *
 * @param value - The value to check
 * @returns True if the value appears to be a MongoDB Binary
 * @internal
 */
function isBinaryLike(value: unknown): boolean {
  if (typeof value !== 'object' || value === null) {
    return false;
  }

  const obj = value as Record<string, unknown>;

  // Check for sub_type property (MongoDB Binary signature)
  if ('sub_type' in obj && typeof obj.sub_type === 'number' && 'buffer' in obj) {
    return true;
  }

  return false;
}

// ============================================================================
// Type Promotion Rules
// ============================================================================

/**
 * Valid type promotions map.
 * Key is the source type, value is array of valid target types.
 */
const TYPE_PROMOTIONS: Map<BSONTypeValue, BSONTypeValue[]> = new Map([
  // Numeric widening
  [BSONType.INT32, [BSONType.INT64, BSONType.DOUBLE]],
  [BSONType.INT64, [BSONType.DOUBLE]],

  // Date/timestamp interchangeable
  [BSONType.DATE, [BSONType.TIMESTAMP]],
  [BSONType.TIMESTAMP, [BSONType.DATE]],

  // ObjectId can be represented as string
  [BSONType.OBJECT_ID, [BSONType.STRING]],
]);

/**
 * Check if one BSON type can be safely promoted to another.
 *
 * @param from - Source type
 * @param to - Target type
 * @returns True if promotion is safe and lossless
 */
export function canPromoteType(from: BSONTypeValue, to: BSONTypeValue): boolean {
  // Same type is always valid
  if (from === to) {
    return true;
  }

  const validTargets = TYPE_PROMOTIONS.get(from);
  return validTargets?.includes(to) ?? false;
}

/**
 * Get the promoted type mapping when combining two type mappings.
 * Returns the wider type that can represent both.
 *
 * @param mapping1 - First type mapping
 * @param mapping2 - Second type mapping
 * @returns The promoted mapping
 */
export function getPromotedMapping(
  mapping1: TypeMapping,
  mapping2: TypeMapping
): TypeMapping {
  // If either is variant, result is variant
  if (mapping1.isVariant || mapping2.isVariant) {
    return {
      physicalType: ParquetPhysicalType.BYTE_ARRAY,
      isVariant: true,
    };
  }

  // Same physical type
  if (mapping1.physicalType === mapping2.physicalType) {
    // Prefer the one with a logical type if available
    if (mapping1.logicalType) return { ...mapping1 };
    if (mapping2.logicalType) return { ...mapping2 };
    return { ...mapping1 };
  }

  // Check numeric hierarchy
  const physicalToIndex: Record<string, number> = {
    [ParquetPhysicalType.INT32]: 0,
    [ParquetPhysicalType.INT64]: 1,
    [ParquetPhysicalType.DOUBLE]: 2,
  };

  const idx1 = physicalToIndex[mapping1.physicalType];
  const idx2 = physicalToIndex[mapping2.physicalType];

  // Both are numeric types
  if (idx1 !== undefined && idx2 !== undefined) {
    const maxIdx = Math.max(idx1, idx2);
    if (maxIdx === 0) {
      return {
        physicalType: ParquetPhysicalType.INT32,
        logicalType: ParquetLogicalType.INT_32,
      };
    } else if (maxIdx === 1) {
      return {
        physicalType: ParquetPhysicalType.INT64,
        logicalType: ParquetLogicalType.INT_64,
      };
    } else {
      return {
        physicalType: ParquetPhysicalType.DOUBLE,
      };
    }
  }

  // Incompatible types - fall back to variant
  return {
    physicalType: ParquetPhysicalType.BYTE_ARRAY,
    isVariant: true,
  };
}

// ============================================================================
// Schema Mapping
// ============================================================================

/**
 * Map a schema field definition to a Parquet schema field.
 *
 * @param field - The schema field to map
 * @returns The Parquet schema field
 */
function mapFieldToParquet(field: SchemaField): ParquetSchemaField {
  const baseMapping = bsonToParquet(field.type);

  // Handle struct (nested object) with explicit children
  if (field.type === BSONType.OBJECT && field.children) {
    return {
      name: field.name,
      physicalType: baseMapping.physicalType,
      optional: field.optional,
      isStruct: true,
      children: field.children.map(mapFieldToParquet),
    };
  }

  // Handle array (list) type
  if (field.type === BSONType.ARRAY && field.elementType) {
    let elementSchema: ParquetSchemaField;

    // Array of objects
    if (field.elementType === BSONType.OBJECT && field.elementChildren) {
      elementSchema = {
        name: 'element',
        physicalType: ParquetPhysicalType.BYTE_ARRAY,
        optional: field.elementOptional,
        isStruct: true,
        children: field.elementChildren.map(mapFieldToParquet),
      };
    }
    // Nested array
    else if (
      field.elementType === BSONType.ARRAY &&
      field.nestedElementType
    ) {
      const nestedElementMapping = bsonToParquet(field.nestedElementType);
      elementSchema = {
        name: 'element',
        physicalType: ParquetPhysicalType.BYTE_ARRAY,
        optional: field.elementOptional,
        isList: true,
        elementType: {
          name: 'element',
          physicalType: nestedElementMapping.physicalType,
          logicalType: nestedElementMapping.logicalType,
          optional: false,
        },
      };
    }
    // Simple array
    else {
      const elementMapping = bsonToParquet(field.elementType);
      elementSchema = {
        name: 'element',
        physicalType: elementMapping.physicalType,
        logicalType: elementMapping.logicalType,
        optional: field.elementOptional,
        isVariant: elementMapping.isVariant,
      };
    }

    return {
      name: field.name,
      physicalType: baseMapping.physicalType,
      optional: field.optional,
      isList: true,
      elementType: elementSchema,
    };
  }

  // Simple field
  return {
    name: field.name,
    physicalType: baseMapping.physicalType,
    logicalType: baseMapping.logicalType,
    optional: field.optional,
    typeLength: baseMapping.typeLength,
    isVariant: baseMapping.isVariant,
    precision: baseMapping.precision,
    scale: baseMapping.scale,
  };
}

/**
 * Map a complete schema definition to Parquet schema.
 *
 * @param schema - Array of schema field definitions
 * @returns The Parquet schema
 */
export function mapSchemaToParquet(schema: SchemaField[]): ParquetSchema {
  return {
    fields: schema.map(mapFieldToParquet),
  };
}

// ============================================================================
// Value Mapping
// ============================================================================

/**
 * Map a JavaScript value to Parquet format.
 * Detects the type, transforms the value if needed, and returns mapping info.
 *
 * @param value - The value to map
 * @param seen - Set of seen objects for circular reference detection (internal use)
 * @returns The mapped value with type information
 * @throws Error if circular reference is detected
 */
export function mapValueToParquet(value: unknown, seen?: WeakSet<object>): MappedValue {
  // Null handling
  if (value === null || value === undefined) {
    return {
      value: null,
      bsonType: BSONType.NULL,
      isNull: true,
    };
  }

  const bsonType = inferBSONType(value);

  // Special handling for different types
  switch (bsonType) {
    case BSONType.DATE:
      // Convert Date to timestamp (milliseconds)
      return {
        value: (value as Date).getTime(),
        bsonType,
        isNull: false,
      };

    case BSONType.OBJECT_ID:
      // Convert ObjectId to bytes
      return {
        value: objectIdToBytes(value),
        bsonType,
        isNull: false,
      };

    case BSONType.ARRAY:
    case BSONType.OBJECT: {
      // Check for circular references in complex types
      const seenSet = seen ?? new WeakSet<object>();
      const obj = value as object;

      if (seenSet.has(obj)) {
        throw new Error('Circular reference detected in object');
      }
      seenSet.add(obj);

      // Check nested values for circular references
      if (Array.isArray(obj)) {
        for (const item of obj) {
          if (item !== null && typeof item === 'object') {
            // Recursively check for circular references
            mapValueToParquet(item, seenSet);
          }
        }
      } else {
        for (const key of Object.keys(obj)) {
          const val = (obj as Record<string, unknown>)[key];
          if (val !== null && typeof val === 'object') {
            // Recursively check for circular references
            mapValueToParquet(val, seenSet);
          }
        }
      }

      // Complex types use variant encoding
      return {
        value,
        bsonType,
        isNull: false,
        useVariant: true,
      };
    }

    default:
      // Pass through as-is
      return {
        value,
        bsonType,
        isNull: false,
      };
  }
}

/**
 * Convert an ObjectId-like value to a 12-byte Uint8Array.
 */
function objectIdToBytes(value: unknown): Uint8Array {
  const obj = value as { toHexString?: () => string; toString: () => string };

  const hexStr =
    typeof obj.toHexString === 'function'
      ? obj.toHexString()
      : obj.toString();

  const bytes = new Uint8Array(12);
  for (let i = 0; i < 12; i++) {
    bytes[i] = parseInt(hexStr.slice(i * 2, i * 2 + 2), 16);
  }

  return bytes;
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Get the default value for a Parquet type.
 * Used when reading data where a field is missing.
 *
 * @param physicalType - The Parquet physical type
 * @returns The default value
 */
export function getDefaultValue(physicalType: ParquetPhysicalTypeValue): unknown {
  switch (physicalType) {
    case ParquetPhysicalType.BOOLEAN:
      return false;
    case ParquetPhysicalType.INT32:
    case ParquetPhysicalType.INT64:
    case ParquetPhysicalType.FLOAT:
    case ParquetPhysicalType.DOUBLE:
      return 0;
    case ParquetPhysicalType.BYTE_ARRAY:
    case ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY:
      return new Uint8Array(0);
    default:
      return null;
  }
}

/**
 * Check if a physical type is numeric.
 *
 * @param physicalType - The physical type to check
 * @returns True if numeric
 */
export function isNumericPhysicalType(
  physicalType: ParquetPhysicalTypeValue
): boolean {
  const numericTypes: ParquetPhysicalTypeValue[] = [
    ParquetPhysicalType.INT32,
    ParquetPhysicalType.INT64,
    ParquetPhysicalType.INT96,
    ParquetPhysicalType.FLOAT,
    ParquetPhysicalType.DOUBLE,
  ];
  return numericTypes.includes(physicalType);
}

/**
 * Get the byte size of a physical type.
 *
 * @param physicalType - The physical type
 * @param typeLength - Optional fixed length for FIXED_LEN_BYTE_ARRAY
 * @returns The byte size, or -1 for variable length types
 */
export function getPhysicalTypeByteSize(
  physicalType: ParquetPhysicalTypeValue,
  typeLength?: number
): number {
  switch (physicalType) {
    case ParquetPhysicalType.BOOLEAN:
      return 1; // Actually bit-packed, but minimum allocation
    case ParquetPhysicalType.INT32:
    case ParquetPhysicalType.FLOAT:
      return 4;
    case ParquetPhysicalType.INT64:
    case ParquetPhysicalType.DOUBLE:
      return 8;
    case ParquetPhysicalType.INT96:
      return 12;
    case ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY:
      return typeLength ?? -1;
    case ParquetPhysicalType.BYTE_ARRAY:
    default:
      return -1; // Variable length
  }
}

// ============================================================================
// Type Coercion
// ============================================================================

/**
 * Options for type coercion
 */
interface CoerceOptions {
  allowNarrowing?: boolean;
}

/**
 * Coerce a value from one BSON type to another.
 *
 * @param value - The value to coerce
 * @param from - Source BSON type
 * @param to - Target BSON type
 * @param options - Coercion options
 * @returns The coerced value
 * @throws Error if coercion is not possible
 */
export function coerceValue(
  value: unknown,
  from: BSONTypeValue,
  to: BSONTypeValue,
  options?: CoerceOptions
): unknown {
  // Same type - no coercion needed
  if (from === to) {
    return value;
  }

  // Int32 -> Int64
  if (from === BSONType.INT32 && to === BSONType.INT64) {
    return BigInt(value as number);
  }

  // Int32 -> Double
  if (from === BSONType.INT32 && to === BSONType.DOUBLE) {
    return Number(value);
  }

  // Int64 -> Double
  if (from === BSONType.INT64 && to === BSONType.DOUBLE) {
    return Number(value);
  }

  // Int64 -> Int32 (with narrowing check)
  if (from === BSONType.INT64 && to === BSONType.INT32) {
    const bigVal = value as bigint;
    const INT32_MIN = BigInt(-2147483648);
    const INT32_MAX = BigInt(2147483647);

    if (bigVal < INT32_MIN || bigVal > INT32_MAX) {
      throw new Error(`Value ${bigVal} overflows int32 range`);
    }

    if (options?.allowNarrowing) {
      return Number(bigVal);
    }

    throw new Error('Cannot narrow int64 to int32 without allowNarrowing option');
  }

  // Double -> Int32 (not allowed)
  if (from === BSONType.DOUBLE && to === BSONType.INT32) {
    throw new Error('Cannot coerce double to int32');
  }

  // ObjectId -> String
  if (from === BSONType.OBJECT_ID && to === BSONType.STRING) {
    const obj = value as { toHexString?: () => string; toString: () => string };
    return typeof obj.toHexString === 'function' ? obj.toHexString() : obj.toString();
  }

  // Date -> String (ISO format)
  if (from === BSONType.DATE && to === BSONType.STRING) {
    return (value as Date).toISOString();
  }

  // Double/Int32/Int64 -> String
  if ((from === BSONType.DOUBLE || from === BSONType.INT32 || from === BSONType.INT64) && to === BSONType.STRING) {
    return String(value);
  }

  // Boolean -> String
  if (from === BSONType.BOOLEAN && to === BSONType.STRING) {
    return String(value);
  }

  // Date -> Timestamp
  if (from === BSONType.DATE && to === BSONType.TIMESTAMP) {
    return (value as Date).getTime();
  }

  // Timestamp -> Date
  if (from === BSONType.TIMESTAMP && to === BSONType.DATE) {
    return new Date(value as number);
  }

  // String -> Date
  if (from === BSONType.STRING && to === BSONType.DATE) {
    return new Date(value as string);
  }

  throw new Error(`Cannot coerce ${from} to ${to}`);
}

// ============================================================================
// Decimal128 Handling
// ============================================================================

/**
 * Map Decimal128 type to Parquet with custom precision and scale.
 *
 * @param options - Precision and scale options
 * @returns The type mapping
 */
export function mapDecimal128ToParquet(options: { precision: number; scale: number }): TypeMapping {
  return {
    physicalType: ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY,
    logicalType: ParquetLogicalType.DECIMAL,
    typeLength: 16,
    precision: options.precision,
    scale: options.scale,
  };
}

/**
 * Infer precision and scale from a Decimal128 value.
 *
 * @param decimal - The Decimal128-like value
 * @returns Precision and scale
 */
export function inferDecimal128Properties(
  decimal: { toString: () => string }
): { precision: number; scale: number } {
  const str = decimal.toString();

  // Handle special values
  if (str === 'Infinity' || str === '-Infinity' || str === 'NaN') {
    return { precision: 34, scale: 0 };
  }

  // Handle scientific notation
  const sciMatch = str.match(/^-?(\d+)\.?(\d*)E([+-]?\d+)$/i);
  if (sciMatch) {
    const intPart = sciMatch[1]!;
    const fracPart = sciMatch[2] || '';
    const exp = parseInt(sciMatch[3]!, 10);

    // Calculate effective precision
    const significantDigits = intPart.length + fracPart.length;
    const scale = Math.max(0, fracPart.length - exp);
    const precision = Math.max(significantDigits, significantDigits + Math.abs(exp));

    return { precision: Math.min(precision, 34), scale: Math.min(scale, 34) };
  }

  // Handle regular decimal
  const match = str.match(/^-?(\d*)\.?(\d*)$/);
  if (match) {
    const intPart = match[1] || '0';
    const fracPart = match[2] || '';

    const scale = fracPart.length;
    const precision = intPart.length + scale;

    return { precision, scale };
  }

  // Default fallback
  return { precision: 34, scale: 0 };
}

/**
 * Map a Decimal128 value, detecting special values.
 *
 * @param decimal - The Decimal128-like value
 * @returns Mapping result with special value info
 */
export function mapDecimal128Value(
  decimal: { toString: () => string }
): { isSpecial: boolean; specialType?: string } {
  const str = decimal.toString();

  if (str === 'Infinity' || str === '-Infinity') {
    return { isSpecial: true, specialType: 'infinity' };
  }

  if (str === 'NaN') {
    return { isSpecial: true, specialType: 'nan' };
  }

  return { isSpecial: false };
}

/**
 * Convert Decimal128 to 16-byte representation.
 *
 * @param decimal - The Decimal128-like value
 * @returns 16-byte Uint8Array
 */
export function decimal128ToBytes(
  decimal: { toString: () => string; bytes?: Uint8Array }
): Uint8Array {
  // If the decimal already has bytes, use them
  if (decimal.bytes instanceof Uint8Array && decimal.bytes.length === 16) {
    return decimal.bytes;
  }

  // Otherwise create a placeholder (real implementation would parse the string)
  return new Uint8Array(16);
}

// ============================================================================
// Binary Data Handling
// ============================================================================

/**
 * Binary subtypes
 */
const BINARY_SUBTYPE = {
  GENERIC: 0x00,
  FUNCTION: 0x01,
  BINARY_OLD: 0x02,
  UUID_OLD: 0x03,
  UUID: 0x04,
  MD5: 0x05,
  ENCRYPTED: 0x06,
  COLUMN: 0x07,
  USER_DEFINED: 0x80,
} as const;

/**
 * Map binary data to Parquet type based on subtype.
 *
 * @param options - Binary subtype options
 * @returns The type mapping
 */
export function mapBinaryToParquet(options: { subtype: number }): TypeMapping {
  switch (options.subtype) {
    case BINARY_SUBTYPE.UUID:
      return {
        physicalType: ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY,
        logicalType: ParquetLogicalType.UUID,
        typeLength: 16,
      };

    case BINARY_SUBTYPE.MD5:
      return {
        physicalType: ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY,
        typeLength: 16,
      };

    default:
      return {
        physicalType: ParquetPhysicalType.BYTE_ARRAY,
      };
  }
}

/**
 * Map a binary value preserving subtype information.
 *
 * @param binary - The MongoDB Binary-like value
 * @returns Mapped binary with subtype
 */
export function mapBinaryValue(
  binary: { sub_type: number; buffer: Uint8Array }
): { subtype: number; value: Uint8Array } {
  return {
    subtype: binary.sub_type,
    value: binary.buffer,
  };
}

// ============================================================================
// Schema Inference
// ============================================================================

/**
 * Infer schema from a single document.
 *
 * @param doc - The document to infer schema from
 * @returns Array of schema fields
 */
export function inferSchemaFromDocument(
  doc: Record<string, unknown>
): SchemaField[] {
  const fields: SchemaField[] = [];

  for (const [name, value] of Object.entries(doc)) {
    // Skip symbol keys
    if (typeof name === 'symbol') continue;

    const bsonType = inferBSONType(value);

    const field: SchemaField = {
      name,
      type: bsonType,
    };

    // Handle arrays
    if (bsonType === BSONType.ARRAY && Array.isArray(value)) {
      const arrayInfo = inferArrayElementType(value);
      // Only set elementType if it's defined and non-empty (not for empty arrays)
      if (arrayInfo.elementType !== undefined && arrayInfo.elementType !== '') {
        field.elementType = arrayInfo.elementType as BSONTypeValue;
      }
      field.elementOptional = arrayInfo.hasHoles || value.some(v => v === null);
      if (arrayInfo.elementChildren) {
        field.elementChildren = arrayInfo.elementChildren;
      }
      if (arrayInfo.nestedElementType) {
        field.nestedElementType = arrayInfo.nestedElementType as BSONTypeValue;
      }
    }

    // Handle nested objects
    if (bsonType === BSONType.OBJECT && value !== null && typeof value === 'object' && !Array.isArray(value)) {
      // Make sure it's not a special type (ObjectId, Decimal128, Binary)
      if (!isObjectIdLike(value) && !isDecimal128Like(value) && !isBinaryLike(value)) {
        field.children = inferSchemaFromDocument(value as Record<string, unknown>);
      }
    }

    fields.push(field);
  }

  return fields;
}

/**
 * Infer array element type from array values.
 *
 * @param array - The array to analyze
 * @returns Element type info
 */
export function inferArrayElementType(
  array: unknown[]
): {
  isMixed: boolean;
  elementType: string;
  nestedElementType?: string;
  elementChildren?: SchemaField[];
  hasHoles?: boolean;
} {
  // Check for sparse array (holes) - count actual keys vs length
  let hasHoles = false;
  if (array.length > 0) {
    let actualKeys = 0;
    for (let i = 0; i < array.length; i++) {
      if (i in array) {
        actualKeys++;
      }
    }
    hasHoles = actualKeys < array.length;
  }

  // Get non-null/undefined elements
  const elements = array.filter(v => v !== null && v !== undefined);

  if (elements.length === 0) {
    // Empty array - no element type can be inferred
    // Return empty string to indicate unknown type
    return { isMixed: false, elementType: '', hasHoles };
  }

  // Infer types of all elements
  const types = elements.map(inferBSONType);
  const uniqueTypes = [...new Set(types)];

  // Single type
  if (uniqueTypes.length === 1) {
    const elementType = uniqueTypes[0];

    // Array of arrays
    if (elementType === BSONType.ARRAY) {
      const nestedArrays = elements.filter(Array.isArray) as unknown[][];
      if (nestedArrays.length > 0) {
        const nestedInfo = inferArrayElementType(nestedArrays.flat());
        return {
          isMixed: false,
          elementType: BSONType.ARRAY,
          nestedElementType: nestedInfo.elementType,
          hasHoles,
        };
      }
    }

    // Array of objects
    if (elementType === BSONType.OBJECT) {
      const objects = elements.filter(v => typeof v === 'object' && v !== null && !Array.isArray(v)) as Record<string, unknown>[];
      if (objects.length > 0) {
        const schemas = objects.map(inferSchemaFromDocument);
        const mergedChildren = mergeSchemas(schemas);
        return {
          isMixed: false,
          elementType: BSONType.OBJECT,
          elementChildren: mergedChildren,
          hasHoles,
        };
      }
    }

    return { isMixed: false, elementType: elementType!, hasHoles };
  }

  // Check for promotable numeric types
  const numericTypes: BSONTypeValue[] = [BSONType.INT32, BSONType.INT64, BSONType.DOUBLE];
  if (uniqueTypes.every(t => numericTypes.includes(t))) {
    // Find the widest type
    if (uniqueTypes.includes(BSONType.DOUBLE)) {
      return { isMixed: false, elementType: BSONType.DOUBLE, hasHoles };
    }
    if (uniqueTypes.includes(BSONType.INT64)) {
      return { isMixed: false, elementType: BSONType.INT64, hasHoles };
    }
    return { isMixed: false, elementType: BSONType.INT32, hasHoles };
  }

  // Mixed types - use variant
  return { isMixed: true, elementType: BSONType.OBJECT, hasHoles };
}

/**
 * Merge multiple schemas into one, handling type promotion and optionality.
 *
 * @param schemas - Array of schemas to merge
 * @returns Merged schema
 */
export function mergeSchemas(schemas: SchemaField[][]): SchemaField[] {
  if (schemas.length === 0) return [];
  if (schemas.length === 1) return schemas[0]!;

  const fieldMap = new Map<string, { types: BSONTypeValue[]; count: number; fields: SchemaField[] }>();
  const totalDocs = schemas.length;

  for (const schema of schemas) {
    for (const field of schema) {
      const existing = fieldMap.get(field.name);
      if (existing) {
        existing.types.push(field.type);
        existing.count++;
        existing.fields.push(field);
      } else {
        fieldMap.set(field.name, { types: [field.type], count: 1, fields: [field] });
      }
    }
  }

  const result: SchemaField[] = [];

  for (const [name, info] of fieldMap) {
    // Filter out NULL types to get actual types
    const nonNullTypes = info.types.filter(t => t !== BSONType.NULL);
    const hasNullType = info.types.some(t => t === BSONType.NULL);
    const uniqueTypes = [...new Set(nonNullTypes)];

    // Field is optional if it doesn't appear in all docs OR if any occurrence is NULL
    const isOptional = info.count < totalDocs || hasNullType;

    let finalType: BSONTypeValue;
    let children: SchemaField[] | undefined;

    // If all values were null, type is NULL
    if (uniqueTypes.length === 0) {
      finalType = BSONType.NULL;
    } else if (uniqueTypes.length === 1) {
      finalType = uniqueTypes[0]!;

      // Merge children for object types
      if (finalType === BSONType.OBJECT) {
        const childSchemas = info.fields
          .filter(f => f.children)
          .map(f => f.children!);
        if (childSchemas.length > 0) {
          children = mergeSchemas(childSchemas);
        }
      }
    } else {
      // Check for promotable numeric types
      const numericTypesForMerge: BSONTypeValue[] = [BSONType.INT32, BSONType.INT64, BSONType.DOUBLE];
      if (uniqueTypes.every(t => numericTypesForMerge.includes(t))) {
        if (uniqueTypes.includes(BSONType.DOUBLE)) {
          finalType = BSONType.DOUBLE;
        } else if (uniqueTypes.includes(BSONType.INT64)) {
          finalType = BSONType.INT64;
        } else {
          finalType = BSONType.INT32;
        }
      } else {
        // Incompatible types - fall back to variant (OBJECT)
        finalType = BSONType.OBJECT;
      }
    }

    result.push({
      name,
      type: finalType,
      optional: isOptional,
      children,
    });
  }

  return result;
}

/**
 * Convenience function to infer schema from multiple documents.
 *
 * @param docs - Array of documents
 * @returns Merged schema
 */
export function inferSchemaFromDocuments(
  docs: Record<string, unknown>[]
): SchemaField[] {
  if (docs.length === 0) return [];

  const schemas = docs.map(inferSchemaFromDocument);
  return mergeSchemas(schemas);
}

// ============================================================================
// Special Value Handling
// ============================================================================

/**
 * Map BSON special values (MinKey, MaxKey).
 *
 * @param value - The special value
 * @returns Mapped value info
 */
export function mapSpecialValue(
  value: { _bsontype: string }
): { bsonType: BSONTypeValue; isSpecialBoundary: boolean } {
  if (value._bsontype === 'MinKey') {
    return { bsonType: BSONType.MIN_KEY, isSpecialBoundary: true };
  }

  if (value._bsontype === 'MaxKey') {
    return { bsonType: BSONType.MAX_KEY, isSpecialBoundary: true };
  }

  return { bsonType: BSONType.OBJECT, isSpecialBoundary: false };
}

// ============================================================================
// Type Documentation Generation
// ============================================================================

/**
 * Type mapping documentation entry.
 */
export interface TypeMappingDoc {
  /** BSON type identifier */
  bsonType: string;
  /** Human-readable description */
  description: string;
  /** Parquet physical type */
  physicalType: string;
  /** Parquet logical type (if any) */
  logicalType?: string;
  /** Fixed byte length (for fixed-length types) */
  typeLength?: number;
  /** Whether this type uses variant encoding */
  isVariant?: boolean;
  /** Example values */
  examples?: string[];
}

/**
 * Generate documentation for all BSON-to-Parquet type mappings.
 *
 * This utility function generates structured documentation that can be
 * used to create markdown docs, JSON schema, or other documentation formats.
 *
 * @returns Array of type mapping documentation entries
 *
 * @example
 * ```typescript
 * const docs = generateTypeMappingDocs();
 * for (const doc of docs) {
 *   console.log(`${doc.bsonType} -> ${doc.physicalType}`);
 * }
 * ```
 */
export function generateTypeMappingDocs(): TypeMappingDoc[] {
  const docs: TypeMappingDoc[] = [
    {
      bsonType: BSONType.DOUBLE,
      description: 'IEEE 754 double-precision floating point number',
      physicalType: ParquetPhysicalType.DOUBLE,
      examples: ['3.14', '-0.5', 'Infinity', 'NaN'],
    },
    {
      bsonType: BSONType.STRING,
      description: 'UTF-8 encoded string',
      physicalType: ParquetPhysicalType.BYTE_ARRAY,
      logicalType: ParquetLogicalType.STRING,
      examples: ['"hello"', '""', '"unicode: \\u4e2d\\u6587"'],
    },
    {
      bsonType: BSONType.OBJECT,
      description: 'Embedded document (nested object)',
      physicalType: ParquetPhysicalType.BYTE_ARRAY,
      isVariant: true,
      examples: ['{ "name": "Alice" }', '{}'],
    },
    {
      bsonType: BSONType.ARRAY,
      description: 'Array of values',
      physicalType: ParquetPhysicalType.BYTE_ARRAY,
      isVariant: true,
      examples: ['[1, 2, 3]', '[]', '["a", "b"]'],
    },
    {
      bsonType: BSONType.BINARY,
      description: 'Binary data',
      physicalType: ParquetPhysicalType.BYTE_ARRAY,
      examples: ['Buffer.from([0x01, 0x02])'],
    },
    {
      bsonType: BSONType.OBJECT_ID,
      description: 'MongoDB ObjectId (12-byte identifier)',
      physicalType: ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY,
      typeLength: 12,
      examples: ['ObjectId("507f1f77bcf86cd799439011")'],
    },
    {
      bsonType: BSONType.BOOLEAN,
      description: 'Boolean value',
      physicalType: ParquetPhysicalType.BOOLEAN,
      examples: ['true', 'false'],
    },
    {
      bsonType: BSONType.DATE,
      description: 'UTC datetime (milliseconds since epoch)',
      physicalType: ParquetPhysicalType.INT64,
      logicalType: ParquetLogicalType.TIMESTAMP_MILLIS,
      examples: ['new Date("2024-01-15T12:30:00Z")'],
    },
    {
      bsonType: BSONType.NULL,
      description: 'Null value',
      physicalType: ParquetPhysicalType.BYTE_ARRAY,
      isVariant: true,
      examples: ['null'],
    },
    {
      bsonType: BSONType.REGEX,
      description: 'Regular expression',
      physicalType: ParquetPhysicalType.BYTE_ARRAY,
      isVariant: true,
      examples: ['/pattern/i', 'new RegExp("test")'],
    },
    {
      bsonType: BSONType.INT32,
      description: '32-bit signed integer',
      physicalType: ParquetPhysicalType.INT32,
      logicalType: ParquetLogicalType.INT_32,
      examples: ['42', '-100', '2147483647'],
    },
    {
      bsonType: BSONType.TIMESTAMP,
      description: 'MongoDB internal timestamp',
      physicalType: ParquetPhysicalType.INT64,
      logicalType: ParquetLogicalType.TIMESTAMP_MILLIS,
      examples: ['Timestamp(1, 1)'],
    },
    {
      bsonType: BSONType.INT64,
      description: '64-bit signed integer',
      physicalType: ParquetPhysicalType.INT64,
      logicalType: ParquetLogicalType.INT_64,
      examples: ['BigInt(9223372036854775807)'],
    },
    {
      bsonType: BSONType.DECIMAL128,
      description: '128-bit decimal floating point',
      physicalType: ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY,
      logicalType: ParquetLogicalType.DECIMAL,
      typeLength: 16,
      examples: ['Decimal128("99.99")'],
    },
    {
      bsonType: BSONType.MIN_KEY,
      description: 'Minimum key value (for sorting)',
      physicalType: ParquetPhysicalType.BYTE_ARRAY,
      isVariant: true,
      examples: ['MinKey()'],
    },
    {
      bsonType: BSONType.MAX_KEY,
      description: 'Maximum key value (for sorting)',
      physicalType: ParquetPhysicalType.BYTE_ARRAY,
      isVariant: true,
      examples: ['MaxKey()'],
    },
  ];

  // Add custom handler documentation
  for (const handler of typeRegistry.getHandlers()) {
    docs.push({
      bsonType: handler.getBSONType(),
      description: `Custom type: ${handler.name}`,
      physicalType: handler.getMapping(null).physicalType,
      logicalType: handler.getMapping(null).logicalType,
      isVariant: handler.getMapping(null).isVariant,
    });
  }

  return docs;
}

/**
 * Generate a markdown table of type mappings.
 *
 * @returns Markdown-formatted string with type mapping table
 *
 * @example
 * ```typescript
 * const markdown = generateTypeMappingMarkdown();
 * console.log(markdown);
 * ```
 */
export function generateTypeMappingMarkdown(): string {
  const docs = generateTypeMappingDocs();
  const lines: string[] = [
    '# BSON to Parquet Type Mappings',
    '',
    '| BSON Type | Description | Physical Type | Logical Type | Notes |',
    '|-----------|-------------|---------------|--------------|-------|',
  ];

  for (const doc of docs) {
    const notes: string[] = [];
    if (doc.typeLength) {
      notes.push(`${doc.typeLength} bytes`);
    }
    if (doc.isVariant) {
      notes.push('variant encoding');
    }

    lines.push(
      `| ${doc.bsonType} | ${doc.description} | ${doc.physicalType} | ${doc.logicalType ?? '-'} | ${notes.join(', ') || '-'} |`
    );
  }

  return lines.join('\n');
}
