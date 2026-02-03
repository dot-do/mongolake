/**
 * Validation Plugin
 *
 * Provides schema-based validation for documents using JSON Schema
 * or custom validation functions.
 *
 * @example
 * ```typescript
 * import { validationPlugin, createValidationPlugin } from 'mongolake/plugin/builtin';
 *
 * // Create with JSON Schema validation
 * const userValidation = createValidationPlugin({
 *   schemas: {
 *     users: {
 *       type: 'object',
 *       required: ['email', 'name'],
 *       properties: {
 *         email: { type: 'string', format: 'email' },
 *         name: { type: 'string', minLength: 1 },
 *         age: { type: 'number', minimum: 0 },
 *       },
 *     },
 *   },
 * });
 *
 * // Or with custom validators
 * const customValidation = createValidationPlugin({
 *   validators: {
 *     users: (doc) => {
 *       if (!doc.email?.includes('@')) {
 *         return { valid: false, errors: ['Invalid email format'] };
 *       }
 *       return { valid: true };
 *     },
 *   },
 * });
 *
 * registry.register(userValidation);
 *
 * // Invalid documents will be rejected:
 * await collection.insertOne({ name: '' }); // Throws ValidationError
 * ```
 */

import type { Document, Filter, Update } from '../../types.js';
import { definePlugin, type CollectionHookContext, type HookResult } from '../index.js';

/**
 * JSON Schema definition (subset for common validation).
 */
export interface JSONSchema {
  type?: 'object' | 'array' | 'string' | 'number' | 'boolean' | 'null';
  required?: string[];
  properties?: Record<string, JSONSchema>;
  items?: JSONSchema;
  minLength?: number;
  maxLength?: number;
  minimum?: number;
  maximum?: number;
  pattern?: string;
  format?: 'email' | 'uri' | 'date' | 'date-time' | 'uuid';
  enum?: unknown[];
  additionalProperties?: boolean | JSONSchema;
}

/**
 * Validation result from a validator function.
 */
export interface ValidationResult {
  valid: boolean;
  errors?: string[];
}

/**
 * Custom validator function.
 */
export type ValidatorFunction = (
  doc: Document,
  context: { operation: 'insert' | 'update'; collection: string }
) => ValidationResult | Promise<ValidationResult>;

/**
 * Configuration options for the validation plugin.
 */
export interface ValidationPluginOptions {
  /** JSON Schema definitions keyed by collection name */
  schemas?: Record<string, JSONSchema>;
  /** Custom validator functions keyed by collection name */
  validators?: Record<string, ValidatorFunction>;
  /** Whether to validate on insert (default: true) */
  validateOnInsert?: boolean;
  /** Whether to validate on update (default: true) */
  validateOnUpdate?: boolean;
  /** Error handler for custom error reporting */
  onError?: (collection: string, doc: Document, errors: string[]) => void;
}

/**
 * Validation error thrown when a document fails validation.
 */
export class ValidationError extends Error {
  public readonly collection: string;
  public readonly validationErrors: string[];
  public readonly document: Document;

  constructor(collection: string, document: Document, errors: string[]) {
    super(`Validation failed for ${collection}: ${errors.join('; ')}`);
    this.name = 'ValidationError';
    this.collection = collection;
    this.validationErrors = errors;
    this.document = document;
  }
}

/**
 * Basic JSON Schema validator implementation.
 */
function validateAgainstSchema(doc: Document, schema: JSONSchema, path = ''): string[] {
  const errors: string[] = [];

  if (schema.type === 'object' && typeof doc === 'object' && doc !== null) {
    // Check required fields
    if (schema.required) {
      for (const field of schema.required) {
        if (!(field in doc) || doc[field] === undefined) {
          errors.push(`${path ? path + '.' : ''}${field} is required`);
        }
      }
    }

    // Validate properties
    if (schema.properties) {
      for (const [key, propSchema] of Object.entries(schema.properties)) {
        if (key in doc) {
          const propPath = path ? `${path}.${key}` : key;
          const propErrors = validateProperty(doc[key], propSchema, propPath);
          errors.push(...propErrors);
        }
      }
    }

    // Check for additional properties
    if (schema.additionalProperties === false && schema.properties) {
      const allowedKeys = new Set(Object.keys(schema.properties));
      for (const key of Object.keys(doc)) {
        if (!allowedKeys.has(key) && key !== '_id') {
          errors.push(`${path ? path + '.' : ''}${key} is not allowed`);
        }
      }
    }
  }

  return errors;
}

/**
 * Validate a single property value.
 */
function validateProperty(value: unknown, schema: JSONSchema, path: string): string[] {
  const errors: string[] = [];

  // Skip null/undefined if not explicitly required
  if (value === null || value === undefined) {
    return errors;
  }

  // Type validation
  if (schema.type) {
    const actualType = getJSONType(value);
    if (actualType !== schema.type) {
      errors.push(`${path} must be of type ${schema.type}, got ${actualType}`);
      return errors; // Skip further validation if type is wrong
    }
  }

  // String validations
  if (typeof value === 'string') {
    if (schema.minLength !== undefined && value.length < schema.minLength) {
      errors.push(`${path} must be at least ${schema.minLength} characters`);
    }
    if (schema.maxLength !== undefined && value.length > schema.maxLength) {
      errors.push(`${path} must be at most ${schema.maxLength} characters`);
    }
    if (schema.pattern) {
      const regex = new RegExp(schema.pattern);
      if (!regex.test(value)) {
        errors.push(`${path} must match pattern ${schema.pattern}`);
      }
    }
    if (schema.format) {
      const formatError = validateFormat(value, schema.format, path);
      if (formatError) {
        errors.push(formatError);
      }
    }
  }

  // Number validations
  if (typeof value === 'number') {
    if (schema.minimum !== undefined && value < schema.minimum) {
      errors.push(`${path} must be at least ${schema.minimum}`);
    }
    if (schema.maximum !== undefined && value > schema.maximum) {
      errors.push(`${path} must be at most ${schema.maximum}`);
    }
  }

  // Enum validation
  if (schema.enum && !schema.enum.includes(value)) {
    errors.push(`${path} must be one of: ${schema.enum.join(', ')}`);
  }

  // Nested object validation
  if (schema.type === 'object' && typeof value === 'object' && value !== null) {
    errors.push(...validateAgainstSchema(value as Document, schema, path));
  }

  // Array validation
  if (schema.type === 'array' && Array.isArray(value) && schema.items) {
    value.forEach((item, index) => {
      errors.push(...validateProperty(item, schema.items!, `${path}[${index}]`));
    });
  }

  return errors;
}

/**
 * Get JSON Schema type for a value.
 */
function getJSONType(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  return typeof value;
}

/**
 * Validate string format.
 */
function validateFormat(value: string, format: string, path: string): string | null {
  switch (format) {
    case 'email':
      // Basic email validation
      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value)) {
        return `${path} must be a valid email address`;
      }
      break;
    case 'uri':
      try {
        new URL(value);
      } catch {
        return `${path} must be a valid URI`;
      }
      break;
    case 'date':
      if (!/^\d{4}-\d{2}-\d{2}$/.test(value) || isNaN(Date.parse(value))) {
        return `${path} must be a valid date (YYYY-MM-DD)`;
      }
      break;
    case 'date-time':
      if (isNaN(Date.parse(value))) {
        return `${path} must be a valid date-time`;
      }
      break;
    case 'uuid':
      if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)) {
        return `${path} must be a valid UUID`;
      }
      break;
  }
  return null;
}

/**
 * Create a validation plugin with the given options.
 */
export function createValidationPlugin(options: ValidationPluginOptions = {}) {
  const validateOnInsert = options.validateOnInsert !== false;
  const validateOnUpdate = options.validateOnUpdate !== false;

  const validateDocument = async (
    doc: Document,
    collection: string,
    operation: 'insert' | 'update'
  ): Promise<ValidationResult> => {
    const errors: string[] = [];

    // Apply JSON Schema validation
    const schema = options.schemas?.[collection];
    if (schema) {
      errors.push(...validateAgainstSchema(doc, schema));
    }

    // Apply custom validator
    const validator = options.validators?.[collection];
    if (validator) {
      const result = await validator(doc, { operation, collection });
      if (!result.valid && result.errors) {
        errors.push(...result.errors);
      }
    }

    return {
      valid: errors.length === 0,
      errors: errors.length > 0 ? errors : undefined,
    };
  };

  return definePlugin({
    name: 'validation',
    version: '1.0.0',
    description: 'Schema-based document validation using JSON Schema or custom validators',
    tags: ['builtin', 'validation', 'schema'],

    hooks: {
      'collection:beforeInsert': async (
        docs: Document[],
        context: CollectionHookContext
      ): Promise<HookResult<Document[]>> => {
        if (!validateOnInsert) {
          return docs;
        }

        // Check if this collection has validation configured
        const hasSchema = options.schemas?.[context.collection];
        const hasValidator = options.validators?.[context.collection];
        if (!hasSchema && !hasValidator) {
          return docs;
        }

        // Validate each document
        for (const doc of docs) {
          const result = await validateDocument(doc, context.collection, 'insert');
          if (!result.valid && result.errors) {
            if (options.onError) {
              options.onError(context.collection, doc, result.errors);
            }
            throw new ValidationError(context.collection, doc, result.errors);
          }
        }

        return docs;
      },

      'collection:beforeUpdate': async (
        params: { filter: Filter<Document>; update: Update<Document> },
        context: CollectionHookContext
      ) => {
        if (!validateOnUpdate) {
          return params;
        }

        // Check if this collection has validation configured
        const hasSchema = options.schemas?.[context.collection];
        const hasValidator = options.validators?.[context.collection];
        if (!hasSchema && !hasValidator) {
          return params;
        }

        // For updates, we can only validate the $set fields if present
        const update = params.update as Record<string, unknown>;
        if (update.$set && typeof update.$set === 'object') {
          const docToValidate = update.$set as Document;
          const result = await validateDocument(docToValidate, context.collection, 'update');
          if (!result.valid && result.errors) {
            if (options.onError) {
              options.onError(context.collection, docToValidate, result.errors);
            }
            throw new ValidationError(context.collection, docToValidate, result.errors);
          }
        }

        return params;
      },
    },
  });
}

/**
 * Default validation plugin instance (no schemas configured).
 * Use createValidationPlugin() to create a plugin with schemas.
 */
export const validationPlugin = createValidationPlugin();
