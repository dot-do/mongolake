/**
 * Iceberg Schema Evolution Tracker
 *
 * Tracks schema changes across Iceberg snapshots, supporting:
 * - Field additions (backwards compatible)
 * - Field removals (mark as optional first, then remove)
 * - Schema evolution metadata generation
 * - Schema ID management
 *
 * @see https://iceberg.apache.org/spec/#schema-evolution
 */

import type {
  IcebergSchema,
  IcebergType,
  IcebergStructField,
  IcebergListType,
  IcebergMapType,
} from '@dotdo/iceberg';

/** Mutable version of IcebergStructField for internal manipulation */
type MutableField = {
  -readonly [K in keyof IcebergStructField]: IcebergStructField[K];
};

/** Mutable version of IcebergSchema for internal manipulation */
type MutableSchema = {
  -readonly [K in keyof IcebergSchema]: K extends 'fields'
    ? MutableField[]
    : IcebergSchema[K];
};

/** Mutable version of IcebergMapType for internal manipulation */
type MutableMapType = {
  -readonly [K in keyof IcebergMapType]: IcebergMapType[K];
};

// ============================================================================
// Type Definitions
// ============================================================================

/** Types of schema changes that can occur */
export type SchemaChangeType =
  | 'add-field'
  | 'remove-field'
  | 'make-optional'
  | 'rename-field'
  | 'update-doc'
  | 'widen-type';

/** A single schema change */
export interface SchemaChange {
  /** The type of change */
  type: SchemaChangeType;
  /** Field ID affected */
  fieldId: number;
  /** Field name (for add/rename operations) */
  fieldName?: string;
  /** Previous field name (for rename operations) */
  previousName?: string;
  /** Parent field ID (for nested fields, -1 for root) */
  parentFieldId: number;
  /** The new type (for add/widen operations) */
  newType?: IcebergType;
  /** The previous type (for widen operations) */
  previousType?: IcebergType;
  /** Whether the field is required */
  required?: boolean;
  /** Documentation string */
  doc?: string;
  /** Timestamp when the change occurred */
  timestampMs: number;
  /** Snapshot ID where this change was introduced */
  snapshotId?: number;
}

/** Schema evolution metadata for a table */
export interface SchemaEvolutionMetadata {
  /** All schema IDs in order */
  schemaIds: number[];
  /** Mapping from schema ID to schema */
  schemas: Map<number, IcebergSchema>;
  /** Changes between consecutive schema versions */
  changes: Map<number, SchemaChange[]>;
  /** The highest field ID used */
  lastFieldId: number;
  /** Current schema ID */
  currentSchemaId: number;
}

/** Options for adding a field */
export interface AddFieldOptions {
  /** Field name */
  name: string;
  /** Field type */
  type: IcebergType;
  /** Whether the field is required (default: false for compatibility) */
  required?: boolean;
  /** Documentation string */
  doc?: string;
  /** Parent field ID for nested fields (-1 or undefined for root) */
  parentFieldId?: number;
}

/** Options for schema evolution */
export interface SchemaEvolutionOptions {
  /** Snapshot ID for tracking */
  snapshotId?: number;
  /** Custom timestamp (defaults to Date.now()) */
  timestampMs?: number;
}

/** Result of schema comparison */
export interface SchemaComparisonResult {
  /** Whether the schemas are compatible (new schema can read old data) */
  compatible: boolean;
  /** List of changes between schemas */
  changes: SchemaChange[];
  /** Breaking changes that prevent compatibility */
  breakingChanges: SchemaChange[];
}

/** Migration step for transforming data between schema versions */
export interface MigrationStep {
  /** Type of migration operation */
  operation: 'copy' | 'rename' | 'widen' | 'drop' | 'add-default';
  /** Source field ID (if applicable) */
  sourceFieldId?: number;
  /** Target field ID (if applicable) */
  targetFieldId?: number;
  /** Default value for new fields (serialized) */
  defaultValue?: unknown;
  /** Type coercion function name (for widen operations) */
  coercionFn?: string;
}

/** Migration plan between two schema versions */
export interface MigrationPlan {
  /** Source schema ID */
  fromSchemaId: number;
  /** Target schema ID */
  toSchemaId: number;
  /** Ordered list of migration steps */
  steps: MigrationStep[];
  /** Whether this migration is reversible */
  reversible: boolean;
  /** Human-readable description of the migration */
  description: string;
}

/** Constant for root-level parent field ID */
export const ROOT_PARENT_ID = -1;

// ============================================================================
// Type Widening Rules
// ============================================================================

/**
 * Allowed type widenings in Iceberg.
 * Key is the source type, value is an array of types it can be widened to.
 */
const TYPE_WIDENING_RULES: Record<string, string[]> = {
  'int': ['long', 'float', 'double'],
  'long': ['float', 'double'],
  'float': ['double'],
  'fixed': ['binary'],
  'decimal(10,2)': ['decimal(16,2)'],
};

/**
 * Check if a type widening is allowed.
 */
function isTypeWideningAllowed(fromType: IcebergType, toType: IcebergType): boolean {
  const fromStr = typeof fromType === 'string' ? fromType : (fromType as { type: string }).type;
  const toStr = typeof toType === 'string' ? toType : (toType as { type: string }).type;

  if (fromStr === toStr) {
    return true;
  }

  // Handle decimal precision increase
  if (fromStr.startsWith('decimal') && toStr.startsWith('decimal')) {
    const fromMatch = fromStr.match(/decimal\((\d+),(\d+)\)/);
    const toMatch = toStr.match(/decimal\((\d+),(\d+)\)/);
    if (fromMatch && toMatch) {
      const fromPrecision = parseInt(fromMatch[1]!, 10);
      const fromScale = parseInt(fromMatch[2]!, 10);
      const toPrecision = parseInt(toMatch[1]!, 10);
      const toScale = parseInt(toMatch[2]!, 10);
      // Scale must be the same, precision can increase
      return toScale === fromScale && toPrecision >= fromPrecision;
    }
  }

  const allowedWidenings = TYPE_WIDENING_RULES[fromStr];
  return allowedWidenings ? allowedWidenings.includes(toStr) : false;
}

// ============================================================================
// Error Helpers
// ============================================================================

/**
 * Schema tracker error with context information.
 */
export class SchemaTrackerError extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly context?: Record<string, unknown>
  ) {
    super(message);
    this.name = 'SchemaTrackerError';
  }
}

/**
 * Create a descriptive error for field not found.
 */
function fieldNotFoundError(fieldId: number, operation: string): SchemaTrackerError {
  return new SchemaTrackerError(
    `Cannot ${operation}: field with ID ${fieldId} does not exist in the current schema`,
    'FIELD_NOT_FOUND',
    { fieldId, operation }
  );
}

/**
 * Create a descriptive error for required field constraint.
 */
function requiredFieldError(fieldId: number, fieldName: string, operation: string): SchemaTrackerError {
  return new SchemaTrackerError(
    `Cannot ${operation} required field "${fieldName}" (ID: ${fieldId}): ` +
      `mark the field as optional first using makeFieldOptional(${fieldId})`,
    'REQUIRED_FIELD_CONSTRAINT',
    { fieldId, fieldName, operation }
  );
}

/**
 * Create a descriptive error for duplicate field name.
 */
function duplicateNameError(name: string, parentFieldId: number): SchemaTrackerError {
  const scope = parentFieldId === ROOT_PARENT_ID ? 'root level' : `parent field ID ${parentFieldId}`;
  return new SchemaTrackerError(
    `Cannot use name "${name}": a field with this name already exists at ${scope}`,
    'DUPLICATE_FIELD_NAME',
    { name, parentFieldId }
  );
}

/**
 * Create a descriptive error for backwards compatibility violation.
 */
function backwardsCompatibilityError(operation: string, reason: string): SchemaTrackerError {
  return new SchemaTrackerError(
    `Cannot ${operation}: ${reason}. This would break backwards compatibility with existing data.`,
    'BACKWARDS_COMPATIBILITY_VIOLATION',
    { operation, reason }
  );
}

// ============================================================================
// Cloning Helpers
// ============================================================================

/**
 * Deep clone an Iceberg schema using structured cloning via JSON.
 * Returns a mutable copy that can be modified.
 */
function cloneSchema(schema: IcebergSchema): MutableSchema {
  return JSON.parse(JSON.stringify(schema)) as MutableSchema;
}

// ============================================================================
// Field Traversal Helpers
// ============================================================================

/**
 * Find the maximum field ID in a schema, including nested fields.
 * Traverses struct fields, list element IDs, and map key/value IDs.
 */
function findMaxFieldId(schema: IcebergSchema): number {
  let maxId = 0;

  function visitFields(fields: readonly IcebergStructField[]): void {
    for (const field of fields) {
      if (field.id > maxId) {
        maxId = field.id;
      }
      visitType(field.type);
    }
  }

  function visitType(type: IcebergType): void {
    if (typeof type === 'string') {
      return;
    }

    if (type.type === 'struct') {
      visitFields(type.fields);
    } else if (type.type === 'list') {
      const listType = type as IcebergListType;
      if (listType['element-id'] > maxId) {
        maxId = listType['element-id'];
      }
      visitType(listType.element);
    } else if (type.type === 'map') {
      const mapType = type as IcebergMapType;
      if (mapType['key-id'] > maxId) {
        maxId = mapType['key-id'];
      }
      if (mapType['value-id'] > maxId) {
        maxId = mapType['value-id'];
      }
      visitType(mapType.key);
      visitType(mapType.value);
    }
  }

  visitFields(schema.fields);
  return maxId;
}

/**
 * Find a field by ID in a list of fields (including nested).
 * Returns the field and its parent field ID (-1 for root).
 */
function findFieldById(
  fields: readonly IcebergStructField[] | MutableField[],
  fieldId: number,
  parentId: number = -1
): { field: IcebergStructField | MutableField; parentId: number } | undefined {
  for (const field of fields) {
    if (field.id === fieldId) {
      return { field, parentId };
    }

    if (typeof field.type !== 'string' && field.type.type === 'struct') {
      const nested = findFieldById(field.type.fields as readonly IcebergStructField[], fieldId, field.id);
      if (nested) {
        return nested;
      }
    }
  }
  return undefined;
}

/**
 * Find field names at a given scope (root or within a struct).
 */
function getFieldNamesInScope(
  fields: readonly IcebergStructField[] | MutableField[],
  parentFieldId: number
): Set<string> {
  const names = new Set<string>();

  if (parentFieldId === -1) {
    // Root level
    for (const field of fields) {
      names.add(field.name);
    }
  } else {
    // Find the parent struct and get its field names
    const found = findFieldById(fields, parentFieldId);
    if (found && typeof found.field.type !== 'string' && found.field.type.type === 'struct') {
      for (const nested of found.field.type.fields) {
        names.add(nested.name);
      }
    }
  }

  return names;
}

/**
 * Remove a field by ID from a mutable list of fields.
 */
function removeFieldById(fields: MutableField[], fieldId: number): boolean {
  for (let i = 0; i < fields.length; i++) {
    if (fields[i]!.id === fieldId) {
      fields.splice(i, 1);
      return true;
    }

    const field = fields[i]!;
    if (typeof field.type !== 'string' && field.type.type === 'struct') {
      if (removeFieldById(field.type.fields as MutableField[], fieldId)) {
        return true;
      }
    }
  }
  return false;
}

/**
 * Add a field to a parent struct or root (mutable fields).
 */
function addFieldToParent(
  fields: MutableField[],
  parentFieldId: number,
  newField: MutableField
): boolean {
  if (parentFieldId === -1) {
    // Add to root
    fields.push(newField);
    return true;
  }

  // Find parent struct and add to it
  for (const field of fields) {
    if (field.id === parentFieldId) {
      if (typeof field.type !== 'string' && field.type.type === 'struct') {
        (field.type.fields as MutableField[]).push(newField);
        return true;
      }
      return false;
    }

    if (typeof field.type !== 'string' && field.type.type === 'struct') {
      if (addFieldToParent(field.type.fields as MutableField[], parentFieldId, newField)) {
        return true;
      }
    }
  }

  return false;
}

/** Mutable version of Iceberg types for internal manipulation */
type MutableType = IcebergType | {
  type: 'struct';
  fields: MutableField[];
} | {
  type: 'list';
  'element-id': number;
  element: MutableType;
  'element-required'?: boolean;
} | {
  type: 'map';
  'key-id': number;
  'value-id': number;
  key: MutableType;
  value: MutableType;
  'value-required'?: boolean;
};

/**
 * Assign field IDs to a mutable type, starting from the given ID.
 * Returns the next available ID.
 */
function assignFieldIds(type: MutableType, startId: number): number {
  if (typeof type === 'string') {
    return startId;
  }

  let nextId = startId;

  if (type.type === 'struct') {
    for (const field of type.fields as MutableField[]) {
      if (field.id === 0) {
        field.id = nextId++;
      }
      nextId = assignFieldIds(field.type as MutableType, nextId);
    }
  } else if (type.type === 'list') {
    const listType = type as { type: 'list'; 'element-id': number; element: MutableType };
    if (listType['element-id'] === 0) {
      listType['element-id'] = nextId++;
    }
    nextId = assignFieldIds(listType.element, nextId);
  } else if (type.type === 'map') {
    const mapType = type as MutableMapType;
    if (mapType['key-id'] === 0) {
      mapType['key-id'] = nextId++;
    }
    if (mapType['value-id'] === 0) {
      mapType['value-id'] = nextId++;
    }
    nextId = assignFieldIds(mapType.key, nextId);
    nextId = assignFieldIds(mapType.value, nextId);
  }

  return nextId;
}

// ============================================================================
// SchemaTracker Class
// ============================================================================

/**
 * Tracks schema evolution for Iceberg tables.
 *
 * The SchemaTracker maintains a history of schema versions and the changes
 * between them, ensuring that schema evolution follows Iceberg's compatibility
 * rules:
 * - New fields must be optional (for backwards compatibility)
 * - Fields must be marked optional before removal
 * - Type widening is allowed (e.g., int -> long)
 * - Field renaming preserves the field ID
 */
export class SchemaTracker {
  private schemas: Map<number, IcebergSchema> = new Map();
  private changes: Map<number, SchemaChange[]> = new Map();
  private schemaIds: number[] = [];
  private currentSchemaId: number = 0;
  private lastFieldId: number = 0;

  constructor(initialSchema?: IcebergSchema) {
    if (initialSchema) {
      this.schemas.set(initialSchema['schema-id'], cloneSchema(initialSchema));
      this.schemaIds.push(initialSchema['schema-id']);
      this.currentSchemaId = initialSchema['schema-id'];
      this.lastFieldId = findMaxFieldId(initialSchema);
    }
  }

  /**
   * Create a SchemaTracker from existing evolution metadata.
   */
  static fromMetadata(metadata: SchemaEvolutionMetadata): SchemaTracker {
    const tracker = new SchemaTracker();
    tracker.schemaIds = [...metadata.schemaIds];
    tracker.currentSchemaId = metadata.currentSchemaId;
    tracker.lastFieldId = metadata.lastFieldId;

    for (const [id, schema] of metadata.schemas) {
      tracker.schemas.set(id, cloneSchema(schema));
    }

    for (const [id, changeList] of metadata.changes) {
      tracker.changes.set(id, [...changeList]);
    }

    return tracker;
  }

  /**
   * Create a SchemaTracker from an array of schemas (e.g., from table metadata).
   */
  static fromSchemas(schemas: IcebergSchema[], currentSchemaId: number): SchemaTracker {
    const tracker = new SchemaTracker();

    for (const schema of schemas) {
      tracker.schemas.set(schema['schema-id'], cloneSchema(schema));
      tracker.schemaIds.push(schema['schema-id']);
      const maxId = findMaxFieldId(schema);
      if (maxId > tracker.lastFieldId) {
        tracker.lastFieldId = maxId;
      }
    }

    tracker.schemaIds.sort((a, b) => a - b);
    tracker.currentSchemaId = currentSchemaId;

    return tracker;
  }

  /**
   * Get the current schema.
   */
  getCurrentSchema(): IcebergSchema | undefined {
    return this.schemas.get(this.currentSchemaId);
  }

  /**
   * Get a schema by ID.
   */
  getSchema(schemaId: number): IcebergSchema | undefined {
    return this.schemas.get(schemaId);
  }

  /**
   * Get all schemas.
   */
  getAllSchemas(): IcebergSchema[] {
    return this.schemaIds.map((id) => this.schemas.get(id)!);
  }

  /**
   * Get the current schema ID.
   */
  getCurrentSchemaId(): number {
    return this.currentSchemaId;
  }

  /**
   * Get the next available field ID.
   */
  getNextFieldId(): number {
    return this.lastFieldId + 1;
  }

  /**
   * Get the last used field ID.
   */
  getLastFieldId(): number {
    return this.lastFieldId;
  }

  /**
   * Add a new field to the schema.
   *
   * New fields must be optional for backwards compatibility with existing data.
   * This creates a new schema version.
   *
   * @returns The new schema and the assigned field ID
   */
  addField(
    options: AddFieldOptions,
    evolutionOptions: SchemaEvolutionOptions = {}
  ): { schema: IcebergSchema; fieldId: number } {
    if (options.required === true) {
      throw backwardsCompatibilityError(
        `add field "${options.name}"`,
        'new fields must be optional to maintain compatibility with existing data'
      );
    }

    const currentSchema = this.getCurrentSchema();
    if (!currentSchema) {
      throw new SchemaTrackerError(
        'Cannot add field: no current schema exists. Initialize the tracker with a schema first.',
        'NO_CURRENT_SCHEMA',
        { operation: 'addField', fieldName: options.name }
      );
    }

    const parentFieldId = options.parentFieldId ?? ROOT_PARENT_ID;

    // Check for duplicate name at the target scope
    const existingNames = getFieldNamesInScope(currentSchema.fields, parentFieldId);
    if (existingNames.has(options.name)) {
      throw duplicateNameError(options.name, parentFieldId);
    }

    const newSchema = cloneSchema(currentSchema);
    const newSchemaId = this.currentSchemaId + 1;
    newSchema['schema-id'] = newSchemaId;

    const fieldId = this.lastFieldId + 1;

    // Clone and process the type to assign field IDs
    let fieldType: IcebergType;
    if (typeof options.type === 'string') {
      fieldType = options.type;
    } else {
      fieldType = JSON.parse(JSON.stringify(options.type));
      this.lastFieldId = assignFieldIds(fieldType, fieldId + 1) - 1;
    }

    const newField: MutableField = {
      id: fieldId,
      name: options.name,
      required: false,
      type: fieldType,
    };

    if (options.doc) {
      newField.doc = options.doc;
    }

    if (!addFieldToParent(newSchema.fields, parentFieldId, newField)) {
      throw fieldNotFoundError(parentFieldId, `add field "${options.name}" to parent`);
    }

    // Update last field ID
    const newMaxId = findMaxFieldId(newSchema);
    if (newMaxId > this.lastFieldId) {
      this.lastFieldId = newMaxId;
    }

    // Record the change
    const change: SchemaChange = {
      type: 'add-field',
      fieldId,
      fieldName: options.name,
      parentFieldId,
      newType: fieldType,
      required: false,
      timestampMs: evolutionOptions.timestampMs ?? Date.now(),
      snapshotId: evolutionOptions.snapshotId,
    };

    if (options.doc) {
      change.doc = options.doc;
    }

    this.schemas.set(newSchemaId, newSchema);
    this.schemaIds.push(newSchemaId);
    this.changes.set(newSchemaId, [change]);
    this.currentSchemaId = newSchemaId;

    return { schema: newSchema, fieldId };
  }

  /**
   * Mark a field as optional (preparation for removal).
   *
   * Fields must be made optional before they can be removed.
   * This is a safe operation that maintains backwards compatibility.
   */
  makeFieldOptional(fieldId: number, evolutionOptions: SchemaEvolutionOptions = {}): IcebergSchema {
    const currentSchema = this.getCurrentSchema();
    if (!currentSchema) {
      throw new SchemaTrackerError(
        'Cannot make field optional: no current schema exists',
        'NO_CURRENT_SCHEMA',
        { operation: 'makeFieldOptional', fieldId }
      );
    }

    const found = findFieldById(currentSchema.fields, fieldId);
    if (!found) {
      throw fieldNotFoundError(fieldId, 'make optional');
    }

    if (!found.field.required) {
      // Already optional, return current schema (idempotent operation)
      return currentSchema;
    }

    const newSchema = cloneSchema(currentSchema);
    const newSchemaId = this.currentSchemaId + 1;
    newSchema['schema-id'] = newSchemaId;

    // Find and update the field in the new schema
    const foundInNew = findFieldById(newSchema.fields, fieldId);
    if (foundInNew) {
      (foundInNew.field as MutableField).required = false;
    }

    // Record the change
    const change: SchemaChange = {
      type: 'make-optional',
      fieldId,
      parentFieldId: found.parentId,
      timestampMs: evolutionOptions.timestampMs ?? Date.now(),
      snapshotId: evolutionOptions.snapshotId,
    };

    this.schemas.set(newSchemaId, newSchema);
    this.schemaIds.push(newSchemaId);
    this.changes.set(newSchemaId, [change]);
    this.currentSchemaId = newSchemaId;

    return newSchema;
  }

  /**
   * Remove a field from the schema.
   *
   * The field must be optional before it can be removed.
   * Removing a required field would break backwards compatibility.
   */
  removeField(fieldId: number, evolutionOptions: SchemaEvolutionOptions = {}): IcebergSchema {
    const currentSchema = this.getCurrentSchema();
    if (!currentSchema) {
      throw new SchemaTrackerError(
        'Cannot remove field: no current schema exists',
        'NO_CURRENT_SCHEMA',
        { operation: 'removeField', fieldId }
      );
    }

    const found = findFieldById(currentSchema.fields, fieldId);
    if (!found) {
      throw fieldNotFoundError(fieldId, 'remove');
    }

    if (found.field.required) {
      throw requiredFieldError(fieldId, found.field.name, 'remove');
    }

    const newSchema = cloneSchema(currentSchema);
    const newSchemaId = this.currentSchemaId + 1;
    newSchema['schema-id'] = newSchemaId;

    removeFieldById(newSchema.fields, fieldId);

    // Record the change
    const change: SchemaChange = {
      type: 'remove-field',
      fieldId,
      fieldName: found.field.name,
      parentFieldId: found.parentId,
      timestampMs: evolutionOptions.timestampMs ?? Date.now(),
      snapshotId: evolutionOptions.snapshotId,
    };

    this.schemas.set(newSchemaId, newSchema);
    this.schemaIds.push(newSchemaId);
    this.changes.set(newSchemaId, [change]);
    this.currentSchemaId = newSchemaId;

    return newSchema;
  }

  /**
   * Rename a field.
   *
   * Renaming preserves the field ID, ensuring data continuity.
   */
  renameField(
    fieldId: number,
    newName: string,
    evolutionOptions: SchemaEvolutionOptions = {}
  ): IcebergSchema {
    const currentSchema = this.getCurrentSchema();
    if (!currentSchema) {
      throw new SchemaTrackerError(
        'Cannot rename field: no current schema exists',
        'NO_CURRENT_SCHEMA',
        { operation: 'renameField', fieldId, newName }
      );
    }

    const found = findFieldById(currentSchema.fields, fieldId);
    if (!found) {
      throw fieldNotFoundError(fieldId, `rename to "${newName}"`);
    }

    // Check for duplicate names in the same scope (skip if renaming to same name)
    if (found.field.name !== newName) {
      const existingNames = getFieldNamesInScope(currentSchema.fields, found.parentId);
      if (existingNames.has(newName)) {
        throw duplicateNameError(newName, found.parentId);
      }
    }

    const previousName = found.field.name;

    const newSchema = cloneSchema(currentSchema);
    const newSchemaId = this.currentSchemaId + 1;
    newSchema['schema-id'] = newSchemaId;

    // Find and update the field in the new schema
    const foundInNew = findFieldById(newSchema.fields, fieldId);
    if (foundInNew) {
      (foundInNew.field as MutableField).name = newName;
    }

    // Record the change
    const change: SchemaChange = {
      type: 'rename-field',
      fieldId,
      fieldName: newName,
      previousName,
      parentFieldId: found.parentId,
      timestampMs: evolutionOptions.timestampMs ?? Date.now(),
      snapshotId: evolutionOptions.snapshotId,
    };

    this.schemas.set(newSchemaId, newSchema);
    this.schemaIds.push(newSchemaId);
    this.changes.set(newSchemaId, [change]);
    this.currentSchemaId = newSchemaId;

    return newSchema;
  }

  /**
   * Update field documentation.
   */
  updateFieldDoc(
    fieldId: number,
    doc: string,
    evolutionOptions: SchemaEvolutionOptions = {}
  ): IcebergSchema {
    const currentSchema = this.getCurrentSchema();
    if (!currentSchema) {
      throw new SchemaTrackerError(
        'Cannot update field documentation: no current schema exists',
        'NO_CURRENT_SCHEMA',
        { operation: 'updateFieldDoc', fieldId }
      );
    }

    const found = findFieldById(currentSchema.fields, fieldId);
    if (!found) {
      throw fieldNotFoundError(fieldId, 'update documentation for');
    }

    const newSchema = cloneSchema(currentSchema);
    const newSchemaId = this.currentSchemaId + 1;
    newSchema['schema-id'] = newSchemaId;

    // Find and update the field in the new schema
    const foundInNew = findFieldById(newSchema.fields, fieldId);
    if (foundInNew) {
      (foundInNew.field as MutableField).doc = doc;
    }

    // Record the change
    const change: SchemaChange = {
      type: 'update-doc',
      fieldId,
      parentFieldId: found.parentId,
      doc,
      timestampMs: evolutionOptions.timestampMs ?? Date.now(),
      snapshotId: evolutionOptions.snapshotId,
    };

    this.schemas.set(newSchemaId, newSchema);
    this.schemaIds.push(newSchemaId);
    this.changes.set(newSchemaId, [change]);
    this.currentSchemaId = newSchemaId;

    return newSchema;
  }

  /**
   * Set a new schema directly (for bulk updates).
   * This validates the schema against the previous version.
   */
  setSchema(
    schema: IcebergSchema,
    evolutionOptions: SchemaEvolutionOptions = {}
  ): SchemaComparisonResult {
    const currentSchema = this.getCurrentSchema();

    // If no current schema, just set this one
    if (!currentSchema) {
      this.schemas.set(schema['schema-id'], cloneSchema(schema));
      this.schemaIds.push(schema['schema-id']);
      this.currentSchemaId = schema['schema-id'];
      this.lastFieldId = findMaxFieldId(schema);
      return { compatible: true, changes: [], breakingChanges: [] };
    }

    // Compare schemas
    const result = this.compareSchemas(currentSchema, schema);

    // Update the schema regardless of compatibility
    this.schemas.set(schema['schema-id'], cloneSchema(schema));
    this.schemaIds.push(schema['schema-id']);

    // Store changes with timestamp
    const changesWithTimestamp = result.changes.map((c) => ({
      ...c,
      timestampMs: evolutionOptions.timestampMs ?? Date.now(),
      snapshotId: evolutionOptions.snapshotId,
    }));
    this.changes.set(schema['schema-id'], changesWithTimestamp);

    this.currentSchemaId = schema['schema-id'];

    // Update lastFieldId
    const newMaxId = findMaxFieldId(schema);
    if (newMaxId > this.lastFieldId) {
      this.lastFieldId = newMaxId;
    }

    return result;
  }

  /**
   * Compare two schemas and identify changes.
   */
  compareSchemas(oldSchema: IcebergSchema, newSchema: IcebergSchema): SchemaComparisonResult {
    return compareSchemas(oldSchema, newSchema);
  }

  /**
   * Get changes for a specific schema version.
   */
  getChanges(schemaId: number): SchemaChange[] {
    return this.changes.get(schemaId) ?? [];
  }

  /**
   * Get all changes across all schema versions.
   */
  getAllChanges(): SchemaChange[] {
    const allChanges: SchemaChange[] = [];
    for (const schemaId of this.schemaIds) {
      const changes = this.changes.get(schemaId);
      if (changes) {
        allChanges.push(...changes);
      }
    }
    return allChanges;
  }

  /**
   * Get the evolution metadata.
   */
  getEvolutionMetadata(): SchemaEvolutionMetadata {
    return {
      schemaIds: [...this.schemaIds],
      schemas: new Map(this.schemas),
      changes: new Map(this.changes),
      lastFieldId: this.lastFieldId,
      currentSchemaId: this.currentSchemaId,
    };
  }

  /**
   * Generate a summary of schema evolution history.
   */
  getEvolutionSummary(): {
    schemaCount: number;
    fieldAdditions: number;
    fieldRemovals: number;
    currentFieldCount: number;
    history: Array<{
      schemaId: number;
      changeCount: number;
      changes: SchemaChange[];
    }>;
  } {
    let fieldAdditions = 0;
    let fieldRemovals = 0;

    const history: Array<{
      schemaId: number;
      changeCount: number;
      changes: SchemaChange[];
    }> = [];

    for (const schemaId of this.schemaIds) {
      const changes = this.changes.get(schemaId) ?? [];
      for (const change of changes) {
        if (change.type === 'add-field') {
          fieldAdditions++;
        } else if (change.type === 'remove-field') {
          fieldRemovals++;
        }
      }
      history.push({
        schemaId,
        changeCount: changes.length,
        changes,
      });
    }

    const currentSchema = this.getCurrentSchema();
    const currentFieldCount = currentSchema ? countFields(currentSchema.fields) : 0;

    return {
      schemaCount: this.schemaIds.length,
      fieldAdditions,
      fieldRemovals,
      currentFieldCount,
      history,
    };
  }
}

/**
 * Count the number of top-level fields in a schema.
 */
function countFields(fields: readonly IcebergStructField[]): number {
  return fields.length;
}

/**
 * Compare two schemas and identify all changes.
 */
function compareSchemas(oldSchema: IcebergSchema, newSchema: IcebergSchema): SchemaComparisonResult {
  const changes: SchemaChange[] = [];
  const breakingChanges: SchemaChange[] = [];

  // Build field maps by ID
  const oldFieldsById = new Map<number, { field: IcebergStructField; parentId: number }>();
  const newFieldsById = new Map<number, { field: IcebergStructField; parentId: number }>();

  function collectFields(
    fields: readonly IcebergStructField[],
    map: Map<number, { field: IcebergStructField; parentId: number }>,
    parentId: number = -1
  ): void {
    for (const field of fields) {
      map.set(field.id, { field, parentId });
      if (typeof field.type !== 'string' && field.type.type === 'struct') {
        collectFields(field.type.fields as readonly IcebergStructField[], map, field.id);
      }
    }
  }

  collectFields(oldSchema.fields, oldFieldsById);
  collectFields(newSchema.fields, newFieldsById);

  // Check for added fields
  for (const [id, { field, parentId }] of newFieldsById) {
    if (!oldFieldsById.has(id)) {
      const change: SchemaChange = {
        type: 'add-field',
        fieldId: id,
        fieldName: field.name,
        parentFieldId: parentId,
        newType: field.type,
        required: field.required,
        timestampMs: Date.now(),
      };
      changes.push(change);

      // Adding a required field is a breaking change
      if (field.required) {
        breakingChanges.push(change);
      }
    }
  }

  // Check for removed fields
  for (const [id, { field, parentId }] of oldFieldsById) {
    if (!newFieldsById.has(id)) {
      const change: SchemaChange = {
        type: 'remove-field',
        fieldId: id,
        fieldName: field.name,
        parentFieldId: parentId,
        timestampMs: Date.now(),
      };
      changes.push(change);
      // Removing fields is not a breaking change in Iceberg (data still readable)
    }
  }

  // Check for changes to existing fields
  for (const [id, { field: oldField, parentId }] of oldFieldsById) {
    const newEntry = newFieldsById.get(id);
    if (!newEntry) {
      continue;
    }

    const newField = newEntry.field;

    // Check for rename
    if (oldField.name !== newField.name) {
      changes.push({
        type: 'rename-field',
        fieldId: id,
        fieldName: newField.name,
        previousName: oldField.name,
        parentFieldId: parentId,
        timestampMs: Date.now(),
      });
    }

    // Check for required -> optional
    if (oldField.required && !newField.required) {
      changes.push({
        type: 'make-optional',
        fieldId: id,
        parentFieldId: parentId,
        timestampMs: Date.now(),
      });
    }

    // Check for optional -> required (breaking change)
    if (!oldField.required && newField.required) {
      const change: SchemaChange = {
        type: 'make-optional', // Technically the inverse, but we track it
        fieldId: id,
        parentFieldId: parentId,
        timestampMs: Date.now(),
      };
      breakingChanges.push(change);
    }

    // Check for type changes
    const oldTypeStr = typeof oldField.type === 'string' ? oldField.type : JSON.stringify(oldField.type);
    const newTypeStr = typeof newField.type === 'string' ? newField.type : JSON.stringify(newField.type);

    if (oldTypeStr !== newTypeStr) {
      if (isTypeWideningAllowed(oldField.type, newField.type)) {
        changes.push({
          type: 'widen-type',
          fieldId: id,
          parentFieldId: parentId,
          previousType: oldField.type,
          newType: newField.type,
          timestampMs: Date.now(),
        });
      } else {
        // Type change that's not a valid widening is a breaking change
        const change: SchemaChange = {
          type: 'widen-type',
          fieldId: id,
          parentFieldId: parentId,
          previousType: oldField.type,
          newType: newField.type,
          timestampMs: Date.now(),
        };
        changes.push(change);
        breakingChanges.push(change);
      }
    }
  }

  return {
    compatible: breakingChanges.length === 0,
    changes,
    breakingChanges,
  };
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Create a schema tracker from table metadata schemas.
 */
export function createSchemaTracker(
  schemas: IcebergSchema[],
  currentSchemaId: number
): SchemaTracker {
  return SchemaTracker.fromSchemas(schemas, currentSchemaId);
}

/**
 * Validate that a schema evolution is backwards compatible.
 */
export function validateSchemaEvolution(
  oldSchema: IcebergSchema,
  newSchema: IcebergSchema
): SchemaComparisonResult {
  return compareSchemas(oldSchema, newSchema);
}

/**
 * Generate a new schema ID based on existing schemas.
 * Returns 1 if no schemas exist, otherwise returns max ID + 1.
 */
export function generateSchemaId(existingSchemas: IcebergSchema[]): number {
  if (existingSchemas.length === 0) {
    return 1;
  }

  let maxId = 0;
  for (const schema of existingSchemas) {
    if (schema['schema-id'] > maxId) {
      maxId = schema['schema-id'];
    }
  }

  return maxId + 1;
}

// ============================================================================
// Migration Helpers
// ============================================================================

/**
 * Generate a migration plan between two schema versions.
 *
 * The migration plan describes the steps needed to transform data
 * from the old schema to the new schema, including field mappings,
 * type coercions, and default values for new fields.
 *
 * @param tracker - The schema tracker containing both schemas
 * @param fromSchemaId - Source schema ID
 * @param toSchemaId - Target schema ID
 * @returns A migration plan with ordered steps
 */
export function generateMigrationPlan(
  tracker: SchemaTracker,
  fromSchemaId: number,
  toSchemaId: number
): MigrationPlan {
  const fromSchema = tracker.getSchema(fromSchemaId);
  const toSchema = tracker.getSchema(toSchemaId);

  if (!fromSchema) {
    throw new SchemaTrackerError(
      `Source schema with ID ${fromSchemaId} not found`,
      'SCHEMA_NOT_FOUND',
      { schemaId: fromSchemaId }
    );
  }

  if (!toSchema) {
    throw new SchemaTrackerError(
      `Target schema with ID ${toSchemaId} not found`,
      'SCHEMA_NOT_FOUND',
      { schemaId: toSchemaId }
    );
  }

  const comparison = compareSchemas(fromSchema, toSchema);
  const steps: MigrationStep[] = [];
  const changedFieldIds = new Set<number>();

  // Process each change to build migration steps
  for (const change of comparison.changes) {
    changedFieldIds.add(change.fieldId);

    switch (change.type) {
      case 'add-field':
        steps.push({
          operation: 'add-default',
          targetFieldId: change.fieldId,
          defaultValue: null, // New optional fields default to null
        });
        break;

      case 'remove-field':
        steps.push({
          operation: 'drop',
          sourceFieldId: change.fieldId,
        });
        break;

      case 'rename-field':
        steps.push({
          operation: 'rename',
          sourceFieldId: change.fieldId,
          targetFieldId: change.fieldId,
        });
        break;

      case 'widen-type':
        steps.push({
          operation: 'widen',
          sourceFieldId: change.fieldId,
          targetFieldId: change.fieldId,
          coercionFn: getCoercionFunctionName(change.previousType!, change.newType!),
        });
        break;

      case 'make-optional':
        // No data transformation needed
        steps.push({
          operation: 'copy',
          sourceFieldId: change.fieldId,
          targetFieldId: change.fieldId,
        });
        break;

      default:
        // Unknown change type, copy as-is
        steps.push({
          operation: 'copy',
          sourceFieldId: change.fieldId,
          targetFieldId: change.fieldId,
        });
    }
  }

  // Add copy steps for unchanged fields
  const fromFieldIds = collectAllFieldIds(fromSchema);
  const toFieldIds = collectAllFieldIds(toSchema);

  for (const fieldId of toFieldIds) {
    if (!changedFieldIds.has(fieldId) && fromFieldIds.has(fieldId)) {
      steps.unshift({
        operation: 'copy',
        sourceFieldId: fieldId,
        targetFieldId: fieldId,
      });
    }
  }

  // Generate human-readable description
  const addedCount = comparison.changes.filter((c) => c.type === 'add-field').length;
  const removedCount = comparison.changes.filter((c) => c.type === 'remove-field').length;
  const modifiedCount = comparison.changes.filter((c) =>
    ['rename-field', 'widen-type', 'make-optional'].includes(c.type)
  ).length;

  const parts: string[] = [];
  if (addedCount > 0) parts.push(`${addedCount} field(s) added`);
  if (removedCount > 0) parts.push(`${removedCount} field(s) removed`);
  if (modifiedCount > 0) parts.push(`${modifiedCount} field(s) modified`);

  const description =
    parts.length > 0
      ? `Migration from schema ${fromSchemaId} to ${toSchemaId}: ${parts.join(', ')}`
      : `Migration from schema ${fromSchemaId} to ${toSchemaId}: no data changes required`;

  return {
    fromSchemaId,
    toSchemaId,
    steps,
    reversible: comparison.compatible && comparison.breakingChanges.length === 0,
    description,
  };
}

/**
 * Get the coercion function name for a type widening operation.
 */
function getCoercionFunctionName(fromType: IcebergType, toType: IcebergType): string {
  const fromStr = typeof fromType === 'string' ? fromType : (fromType as { type: string }).type;
  const toStr = typeof toType === 'string' ? toType : (toType as { type: string }).type;

  if (fromStr === 'int' && toStr === 'long') return 'intToLong';
  if (fromStr === 'int' && toStr === 'float') return 'intToFloat';
  if (fromStr === 'int' && toStr === 'double') return 'intToDouble';
  if (fromStr === 'long' && toStr === 'float') return 'longToFloat';
  if (fromStr === 'long' && toStr === 'double') return 'longToDouble';
  if (fromStr === 'float' && toStr === 'double') return 'floatToDouble';
  if (fromStr === 'fixed' && toStr === 'binary') return 'fixedToBinary';
  if (fromStr.startsWith('decimal') && toStr.startsWith('decimal')) return 'widenDecimal';

  return 'identity';
}

/**
 * Collect all field IDs from a schema, including nested fields.
 */
function collectAllFieldIds(schema: IcebergSchema): Set<number> {
  const ids = new Set<number>();

  function visitFields(fields: readonly IcebergStructField[]): void {
    for (const field of fields) {
      ids.add(field.id);
      visitType(field.type);
    }
  }

  function visitType(type: IcebergType): void {
    if (typeof type === 'string') return;

    if (type.type === 'struct') {
      visitFields(type.fields);
    } else if (type.type === 'list') {
      const listType = type as IcebergListType;
      ids.add(listType['element-id']);
      visitType(listType.element);
    } else if (type.type === 'map') {
      const mapType = type as IcebergMapType;
      ids.add(mapType['key-id']);
      ids.add(mapType['value-id']);
      visitType(mapType.key);
      visitType(mapType.value);
    }
  }

  visitFields(schema.fields);
  return ids;
}

/**
 * Check if a migration between two schemas is safe (no data loss).
 *
 * A migration is considered safe if:
 * - No required fields are removed
 * - No type narrowing occurs
 * - All type changes are valid widenings
 *
 * @param tracker - The schema tracker
 * @param fromSchemaId - Source schema ID
 * @param toSchemaId - Target schema ID
 * @returns true if migration is safe, false otherwise
 */
export function isMigrationSafe(
  tracker: SchemaTracker,
  fromSchemaId: number,
  toSchemaId: number
): boolean {
  const fromSchema = tracker.getSchema(fromSchemaId);
  const toSchema = tracker.getSchema(toSchemaId);

  if (!fromSchema || !toSchema) {
    return false;
  }

  const comparison = compareSchemas(fromSchema, toSchema);
  return comparison.compatible && comparison.breakingChanges.length === 0;
}

/**
 * Get a human-readable summary of changes between two schemas.
 *
 * @param tracker - The schema tracker
 * @param fromSchemaId - Source schema ID
 * @param toSchemaId - Target schema ID
 * @returns Array of human-readable change descriptions
 */
export function getChangeSummary(
  tracker: SchemaTracker,
  fromSchemaId: number,
  toSchemaId: number
): string[] {
  const fromSchema = tracker.getSchema(fromSchemaId);
  const toSchema = tracker.getSchema(toSchemaId);

  if (!fromSchema || !toSchema) {
    return ['Unable to compare schemas: one or both schemas not found'];
  }

  const comparison = compareSchemas(fromSchema, toSchema);
  const summaries: string[] = [];

  for (const change of comparison.changes) {
    switch (change.type) {
      case 'add-field':
        summaries.push(`Added optional field "${change.fieldName}" (ID: ${change.fieldId})`);
        break;
      case 'remove-field':
        summaries.push(`Removed field "${change.fieldName}" (ID: ${change.fieldId})`);
        break;
      case 'rename-field':
        summaries.push(
          `Renamed field "${change.previousName}" to "${change.fieldName}" (ID: ${change.fieldId})`
        );
        break;
      case 'make-optional':
        summaries.push(`Made field ID ${change.fieldId} optional`);
        break;
      case 'widen-type': {
        const prevType =
          typeof change.previousType === 'string'
            ? change.previousType
            : JSON.stringify(change.previousType);
        const newType =
          typeof change.newType === 'string' ? change.newType : JSON.stringify(change.newType);
        summaries.push(`Widened field ID ${change.fieldId} from ${prevType} to ${newType}`);
        break;
      }
      case 'update-doc':
        summaries.push(`Updated documentation for field ID ${change.fieldId}`);
        break;
    }
  }

  if (comparison.breakingChanges.length > 0) {
    summaries.push(`WARNING: ${comparison.breakingChanges.length} breaking change(s) detected`);
  }

  return summaries.length > 0 ? summaries : ['No changes detected'];
}
