/**
 * Tests for Schema Configuration Parser
 *
 * Tests the parsing and validation of schema configuration for field promotion.
 */

import { describe, it, expect } from 'vitest';
import {
  parseSchemaConfig,
  parseSimpleSchemaConfig,
  parseSchemaConfigFromJSON,
  getColumnForPath,
  isPromotedField,
  getPromotedFieldPaths,
  extractPromotedFields,
  validatePromotedFieldTypes,
  SchemaConfigError,
  type FullSchemaConfig,
  type ParsedSchemaConfig,
  type ParsedCollectionSchema,
} from '../../../src/schema/config.js';

// ============================================================================
// parseSchemaConfig Tests
// ============================================================================

describe('parseSchemaConfig', () => {
  describe('valid configurations', () => {
    it('should parse a basic configuration with string columns', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              _id: 'string',
              email: 'string',
              createdAt: 'timestamp',
            },
          },
        },
      };

      const result = parseSchemaConfig(config);

      expect(result.collections.has('users')).toBe(true);

      const usersSchema = result.collections.get('users')!;
      expect(usersSchema.columns).toHaveLength(3);
      expect(usersSchema.columnMap.get('_id')?.type).toBe('string');
      expect(usersSchema.columnMap.get('email')?.type).toBe('string');
      expect(usersSchema.columnMap.get('createdAt')?.type).toBe('timestamp');
    });

    it('should parse nested field paths', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              'profile.name': 'string',
              'profile.age': 'int32',
              'address.city': 'string',
              'address.zip': 'string',
            },
          },
        },
      };

      const result = parseSchemaConfig(config);
      const usersSchema = result.collections.get('users')!;

      expect(usersSchema.columns).toHaveLength(4);

      const profileName = usersSchema.columnMap.get('profile.name')!;
      expect(profileName.segments).toEqual(['profile', 'name']);
      expect(profileName.type).toBe('string');

      const profileAge = usersSchema.columnMap.get('profile.age')!;
      expect(profileAge.segments).toEqual(['profile', 'age']);
      expect(profileAge.type).toBe('int32');
    });

    it('should parse all Parquet types', () => {
      const config: FullSchemaConfig = {
        collections: {
          test: {
            columns: {
              stringField: 'string',
              int32Field: 'int32',
              int64Field: 'int64',
              floatField: 'float',
              doubleField: 'double',
              booleanField: 'boolean',
              timestampField: 'timestamp',
              dateField: 'date',
              binaryField: 'binary',
              variantField: 'variant',
            },
          },
        },
      };

      const result = parseSchemaConfig(config);
      const schema = result.collections.get('test')!;

      expect(schema.columnMap.get('stringField')?.type).toBe('string');
      expect(schema.columnMap.get('int32Field')?.type).toBe('int32');
      expect(schema.columnMap.get('int64Field')?.type).toBe('int64');
      expect(schema.columnMap.get('floatField')?.type).toBe('float');
      expect(schema.columnMap.get('doubleField')?.type).toBe('double');
      expect(schema.columnMap.get('booleanField')?.type).toBe('boolean');
      expect(schema.columnMap.get('timestampField')?.type).toBe('timestamp');
      expect(schema.columnMap.get('dateField')?.type).toBe('date');
      expect(schema.columnMap.get('binaryField')?.type).toBe('binary');
      expect(schema.columnMap.get('variantField')?.type).toBe('variant');
    });

    it('should parse array types', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              tags: ['string'],
              scores: ['int32'],
              timestamps: ['timestamp'],
            },
          },
        },
      };

      const result = parseSchemaConfig(config);
      const schema = result.collections.get('users')!;

      const tags = schema.columnMap.get('tags')!;
      expect(tags.isArray).toBe(true);
      expect(tags.type).toBe('string');

      const scores = schema.columnMap.get('scores')!;
      expect(scores.isArray).toBe(true);
      expect(scores.type).toBe('int32');
    });

    it('should parse struct types', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              profile: {
                name: 'string',
                age: 'int32',
              },
            },
          },
        },
      };

      const result = parseSchemaConfig(config);
      const schema = result.collections.get('users')!;

      const profile = schema.columnMap.get('profile')!;
      expect(profile.isStruct).toBe(true);
      expect(profile.structDef).toBeDefined();
      expect(profile.structDef!['name'].type).toBe('string');
      expect(profile.structDef!['age'].type).toBe('int32');
    });

    it('should parse autoPromote configuration', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: { _id: 'string' },
            autoPromote: { threshold: 0.9 },
          },
        },
      };

      const result = parseSchemaConfig(config);
      const schema = result.collections.get('users')!;

      expect(schema.autoPromote).toEqual({ threshold: 0.9 });
    });

    it('should default storeVariant to true', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: { _id: 'string' },
          },
        },
      };

      const result = parseSchemaConfig(config);
      const schema = result.collections.get('users')!;

      expect(schema.storeVariant).toBe(true);
    });

    it('should respect storeVariant: false', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: { _id: 'string' },
            storeVariant: false,
          },
        },
      };

      const result = parseSchemaConfig(config);
      const schema = result.collections.get('users')!;

      expect(schema.storeVariant).toBe(false);
    });

    it('should parse multiple collections', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: { _id: 'string', email: 'string' },
          },
          orders: {
            columns: { _id: 'string', total: 'double' },
          },
          products: {
            columns: { _id: 'string', name: 'string', price: 'double' },
          },
        },
      };

      const result = parseSchemaConfig(config);

      expect(result.collections.size).toBe(3);
      expect(result.collections.has('users')).toBe(true);
      expect(result.collections.has('orders')).toBe(true);
      expect(result.collections.has('products')).toBe(true);
    });

    it('should handle empty columns object', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {},
          },
        },
      };

      const result = parseSchemaConfig(config);
      const schema = result.collections.get('users')!;

      expect(schema.columns).toHaveLength(0);
    });

    it('should handle collection without columns', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            autoPromote: { threshold: 0.8 },
          },
        },
      };

      const result = parseSchemaConfig(config);
      const schema = result.collections.get('users')!;

      expect(schema.columns).toHaveLength(0);
      expect(schema.autoPromote?.threshold).toBe(0.8);
    });
  });

  describe('invalid configurations', () => {
    it('should reject null config', () => {
      expect(() => parseSchemaConfig(null as unknown as FullSchemaConfig)).toThrow(
        SchemaConfigError
      );
      expect(() => parseSchemaConfig(null as unknown as FullSchemaConfig)).toThrow(
        'must be an object'
      );
    });

    it('should reject non-object config', () => {
      expect(() => parseSchemaConfig('invalid' as unknown as FullSchemaConfig)).toThrow(
        SchemaConfigError
      );
    });

    it('should reject config without collections', () => {
      expect(() => parseSchemaConfig({} as FullSchemaConfig)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfig({} as FullSchemaConfig)).toThrow(
        'must have a collections object'
      );
    });

    it('should reject invalid Parquet type', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              field: 'invalid_type' as 'string',
            },
          },
        },
      };

      expect(() => parseSchemaConfig(config)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfig(config)).toThrow("Invalid Parquet type 'invalid_type'");
    });

    it('should reject empty field path', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              '': 'string',
            },
          },
        },
      };

      expect(() => parseSchemaConfig(config)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfig(config)).toThrow('must be a non-empty string');
    });

    it('should reject field path with empty segment', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              'profile..name': 'string',
            },
          },
        },
      };

      expect(() => parseSchemaConfig(config)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfig(config)).toThrow('empty segment');
    });

    it('should reject field path starting with number', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              '123field': 'string',
            },
          },
        },
      };

      expect(() => parseSchemaConfig(config)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfig(config)).toThrow('invalid characters');
    });

    it('should reject field path with special characters', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              'field@name': 'string',
            },
          },
        },
      };

      expect(() => parseSchemaConfig(config)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfig(config)).toThrow('invalid characters');
    });

    it('should reject field path exceeding max nesting depth', () => {
      const deepPath = Array(15).fill('level').join('.');
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              [deepPath]: 'string',
            },
          },
        },
      };

      expect(() => parseSchemaConfig(config)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfig(config)).toThrow('exceeds maximum nesting depth');
    });

    it('should reject invalid autoPromote threshold (negative)', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: { _id: 'string' },
            autoPromote: { threshold: -0.1 },
          },
        },
      };

      expect(() => parseSchemaConfig(config)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfig(config)).toThrow('must be between 0 and 1');
    });

    it('should reject invalid autoPromote threshold (greater than 1)', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: { _id: 'string' },
            autoPromote: { threshold: 1.5 },
          },
        },
      };

      expect(() => parseSchemaConfig(config)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfig(config)).toThrow('must be between 0 and 1');
    });

    it('should reject autoPromote without threshold', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: { _id: 'string' },
            autoPromote: {} as { threshold: number },
          },
        },
      };

      expect(() => parseSchemaConfig(config)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfig(config)).toThrow('must have a threshold property');
    });

    it('should reject array with wrong number of elements', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              field: [] as unknown as ['string'],
            },
          },
        },
      };

      expect(() => parseSchemaConfig(config)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfig(config)).toThrow('must have exactly one element type');
    });

    it('should reject invalid array element type', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              field: ['invalid' as 'string'],
            },
          },
        },
      };

      expect(() => parseSchemaConfig(config)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfig(config)).toThrow('Invalid Parquet array element type');
    });
  });
});

// ============================================================================
// parseSimpleSchemaConfig Tests
// ============================================================================

describe('parseSimpleSchemaConfig', () => {
  it('should parse simplified schema format', () => {
    const config = {
      users: {
        columns: {
          _id: 'string' as const,
          email: 'string' as const,
        },
      },
      orders: {
        columns: {
          _id: 'string' as const,
          total: 'double' as const,
        },
      },
    };

    const result = parseSimpleSchemaConfig(config);

    expect(result.collections.size).toBe(2);
    expect(result.collections.has('users')).toBe(true);
    expect(result.collections.has('orders')).toBe(true);
  });

  it('should reject invalid config', () => {
    expect(() => parseSimpleSchemaConfig(null as unknown as Record<string, unknown>)).toThrow(
      SchemaConfigError
    );
  });
});

// ============================================================================
// parseSchemaConfigFromJSON Tests
// ============================================================================

describe('parseSchemaConfigFromJSON', () => {
  it('should parse valid JSON string', () => {
    const json = JSON.stringify({
      collections: {
        users: {
          columns: {
            _id: 'string',
            email: 'string',
          },
        },
      },
    });

    const result = parseSchemaConfigFromJSON(json);

    expect(result.collections.has('users')).toBe(true);
    expect(result.collections.get('users')!.columnMap.get('_id')?.type).toBe('string');
  });

  it('should reject invalid JSON', () => {
    expect(() => parseSchemaConfigFromJSON('not valid json')).toThrow(SchemaConfigError);
    expect(() => parseSchemaConfigFromJSON('not valid json')).toThrow(
      'Failed to parse schema configuration JSON'
    );
  });

  it('should propagate schema validation errors', () => {
    const json = JSON.stringify({
      collections: {
        users: {
          columns: {
            field: 'invalid_type',
          },
        },
      },
    });

    expect(() => parseSchemaConfigFromJSON(json)).toThrow(SchemaConfigError);
    expect(() => parseSchemaConfigFromJSON(json)).toThrow('Invalid Parquet type');
  });
});

// ============================================================================
// Schema Access Utilities Tests
// ============================================================================

describe('getColumnForPath', () => {
  let schema: ParsedCollectionSchema;

  beforeAll(() => {
    const config: FullSchemaConfig = {
      collections: {
        users: {
          columns: {
            _id: 'string',
            email: 'string',
            'profile.name': 'string',
          },
        },
      },
    };
    schema = parseSchemaConfig(config).collections.get('users')!;
  });

  it('should return column for existing path', () => {
    const column = getColumnForPath(schema, '_id');
    expect(column).toBeDefined();
    expect(column!.type).toBe('string');
  });

  it('should return column for nested path', () => {
    const column = getColumnForPath(schema, 'profile.name');
    expect(column).toBeDefined();
    expect(column!.type).toBe('string');
    expect(column!.segments).toEqual(['profile', 'name']);
  });

  it('should return undefined for non-existent path', () => {
    const column = getColumnForPath(schema, 'nonexistent');
    expect(column).toBeUndefined();
  });
});

describe('isPromotedField', () => {
  let schema: ParsedCollectionSchema;

  beforeAll(() => {
    const config: FullSchemaConfig = {
      collections: {
        users: {
          columns: {
            _id: 'string',
            email: 'string',
          },
        },
      },
    };
    schema = parseSchemaConfig(config).collections.get('users')!;
  });

  it('should return true for promoted field', () => {
    expect(isPromotedField(schema, '_id')).toBe(true);
    expect(isPromotedField(schema, 'email')).toBe(true);
  });

  it('should return false for non-promoted field', () => {
    expect(isPromotedField(schema, 'name')).toBe(false);
    expect(isPromotedField(schema, 'age')).toBe(false);
  });
});

describe('getPromotedFieldPaths', () => {
  it('should return all promoted field paths', () => {
    const config: FullSchemaConfig = {
      collections: {
        users: {
          columns: {
            _id: 'string',
            email: 'string',
            'profile.name': 'string',
          },
        },
      },
    };
    const schema = parseSchemaConfig(config).collections.get('users')!;

    const paths = getPromotedFieldPaths(schema);

    expect(paths).toContain('_id');
    expect(paths).toContain('email');
    expect(paths).toContain('profile.name');
    expect(paths).toHaveLength(3);
  });

  it('should return empty array for no columns', () => {
    const config: FullSchemaConfig = {
      collections: {
        users: {},
      },
    };
    const schema = parseSchemaConfig(config).collections.get('users')!;

    const paths = getPromotedFieldPaths(schema);

    expect(paths).toEqual([]);
  });
});

describe('extractPromotedFields', () => {
  let schema: ParsedCollectionSchema;

  beforeAll(() => {
    const config: FullSchemaConfig = {
      collections: {
        users: {
          columns: {
            _id: 'string',
            email: 'string',
            'profile.name': 'string',
            'profile.age': 'int32',
          },
        },
      },
    };
    schema = parseSchemaConfig(config).collections.get('users')!;
  });

  it('should extract promoted field values from document', () => {
    const doc = {
      _id: 'user123',
      email: 'test@example.com',
      profile: {
        name: 'Alice',
        age: 30,
        bio: 'Hello',
      },
      other: 'not promoted',
    };

    const result = extractPromotedFields(schema, doc);

    expect(result.get('_id')).toBe('user123');
    expect(result.get('email')).toBe('test@example.com');
    expect(result.get('profile.name')).toBe('Alice');
    expect(result.get('profile.age')).toBe(30);
    expect(result.size).toBe(4);
  });

  it('should skip undefined values', () => {
    const doc = {
      _id: 'user123',
      // email is missing
      profile: {
        name: 'Alice',
        // age is missing
      },
    };

    const result = extractPromotedFields(schema, doc);

    expect(result.get('_id')).toBe('user123');
    expect(result.has('email')).toBe(false);
    expect(result.get('profile.name')).toBe('Alice');
    expect(result.has('profile.age')).toBe(false);
    expect(result.size).toBe(2);
  });

  it('should handle missing nested parent', () => {
    const doc = {
      _id: 'user123',
      // profile object is missing entirely
    };

    const result = extractPromotedFields(schema, doc);

    expect(result.get('_id')).toBe('user123');
    expect(result.has('profile.name')).toBe(false);
    expect(result.has('profile.age')).toBe(false);
    expect(result.size).toBe(1);
  });
});

// ============================================================================
// validatePromotedFieldTypes Tests
// ============================================================================

describe('validatePromotedFieldTypes', () => {
  let schema: ParsedCollectionSchema;

  beforeAll(() => {
    const config: FullSchemaConfig = {
      collections: {
        users: {
          columns: {
            name: 'string',
            age: 'int32',
            score: 'double',
            active: 'boolean',
            tags: ['string'],
            createdAt: 'timestamp',
          },
        },
      },
    };
    schema = parseSchemaConfig(config).collections.get('users')!;
  });

  it('should return empty array for valid document', () => {
    const doc = {
      name: 'Alice',
      age: 30,
      score: 95.5,
      active: true,
      tags: ['admin', 'user'],
      createdAt: new Date(),
    };

    const errors = validatePromotedFieldTypes(schema, doc);

    expect(errors).toEqual([]);
  });

  it('should skip null and undefined values', () => {
    const doc = {
      name: null,
      age: undefined,
    };

    const errors = validatePromotedFieldTypes(schema, doc as Record<string, unknown>);

    expect(errors).toEqual([]);
  });

  it('should detect string type mismatch', () => {
    const doc = {
      name: 123,
    };

    const errors = validatePromotedFieldTypes(schema, doc);

    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain('name');
    expect(errors[0]).toContain('expected string');
  });

  it('should detect integer type mismatch', () => {
    const doc = {
      age: 'thirty',
    };

    const errors = validatePromotedFieldTypes(schema, doc);

    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain('age');
    expect(errors[0]).toContain('expected integer');
  });

  it('should detect non-integer number as integer type mismatch', () => {
    const doc = {
      age: 30.5,
    };

    const errors = validatePromotedFieldTypes(schema, doc);

    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain('age');
    expect(errors[0]).toContain('expected integer');
  });

  it('should detect boolean type mismatch', () => {
    const doc = {
      active: 'yes',
    };

    const errors = validatePromotedFieldTypes(schema, doc);

    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain('active');
    expect(errors[0]).toContain('expected boolean');
  });

  it('should detect array type mismatch', () => {
    const doc = {
      tags: 'not-an-array',
    };

    const errors = validatePromotedFieldTypes(schema, doc);

    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain('tags');
    expect(errors[0]).toContain('expected array');
  });

  it('should detect array element type mismatch', () => {
    const doc = {
      tags: ['valid', 123, 'another'],
    };

    const errors = validatePromotedFieldTypes(schema, doc);

    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain('tags');
    expect(errors[0]).toContain('element [1]');
    expect(errors[0]).toContain('expected string');
  });

  it('should collect multiple errors', () => {
    const doc = {
      name: 123,
      age: 'invalid',
      active: 'yes',
    };

    const errors = validatePromotedFieldTypes(schema, doc);

    expect(errors).toHaveLength(3);
  });

  it('should accept Date for timestamp fields', () => {
    const doc = {
      createdAt: new Date(),
    };

    const errors = validatePromotedFieldTypes(schema, doc);

    expect(errors).toEqual([]);
  });

  it('should accept string for timestamp fields', () => {
    const doc = {
      createdAt: '2023-01-01T00:00:00Z',
    };

    const errors = validatePromotedFieldTypes(schema, doc);

    expect(errors).toEqual([]);
  });

  it('should accept number for timestamp fields', () => {
    const doc = {
      createdAt: Date.now(),
    };

    const errors = validatePromotedFieldTypes(schema, doc);

    expect(errors).toEqual([]);
  });
});

// ============================================================================
// Integration Test
// ============================================================================

describe('Integration: Complete Schema Configuration', () => {
  it('should handle a real-world schema configuration', () => {
    const config: FullSchemaConfig = {
      collections: {
        users: {
          columns: {
            _id: 'string',
            email: 'string',
            createdAt: 'timestamp',
            'profile.name': 'string',
            'profile.age': 'int32',
            'settings.notifications': 'boolean',
            tags: ['string'],
          },
          autoPromote: { threshold: 0.9 },
          storeVariant: true,
        },
        orders: {
          columns: {
            _id: 'string',
            userId: 'string',
            total: 'double',
            status: 'string',
            createdAt: 'timestamp',
            'shipping.address': 'string',
            'shipping.city': 'string',
            items: ['variant'],
          },
          autoPromote: { threshold: 0.8 },
        },
        products: {
          columns: {
            _id: 'string',
            name: 'string',
            price: 'double',
            inStock: 'boolean',
            categories: ['string'],
          },
          storeVariant: false,
        },
      },
    };

    const result = parseSchemaConfig(config);

    // Verify users collection
    expect(result.collections.has('users')).toBe(true);
    const usersSchema = result.collections.get('users')!;
    expect(usersSchema.columns).toHaveLength(7);
    expect(usersSchema.autoPromote?.threshold).toBe(0.9);
    expect(usersSchema.storeVariant).toBe(true);

    // Verify orders collection
    expect(result.collections.has('orders')).toBe(true);
    const ordersSchema = result.collections.get('orders')!;
    expect(ordersSchema.columns).toHaveLength(8);
    expect(ordersSchema.autoPromote?.threshold).toBe(0.8);

    // Verify products collection
    expect(result.collections.has('products')).toBe(true);
    const productsSchema = result.collections.get('products')!;
    expect(productsSchema.storeVariant).toBe(false);

    // Verify field extraction works
    const userDoc = {
      _id: 'user123',
      email: 'alice@example.com',
      createdAt: new Date(),
      profile: {
        name: 'Alice',
        age: 30,
        bio: 'Developer',
      },
      settings: {
        notifications: true,
        theme: 'dark',
      },
      tags: ['admin', 'developer'],
      extra: 'not promoted',
    };

    const extracted = extractPromotedFields(usersSchema, userDoc);
    expect(extracted.size).toBe(7);
    expect(extracted.get('_id')).toBe('user123');
    expect(extracted.get('profile.name')).toBe('Alice');
    expect(extracted.get('tags')).toEqual(['admin', 'developer']);

    // Verify validation works
    const errors = validatePromotedFieldTypes(usersSchema, userDoc);
    expect(errors).toEqual([]);
  });
});

// ============================================================================
// SchemaConfigError Tests
// ============================================================================

describe('SchemaConfigError', () => {
  it('should be an instance of Error', () => {
    const error = new SchemaConfigError('test message');
    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(SchemaConfigError);
  });

  it('should have correct name', () => {
    const error = new SchemaConfigError('test message');
    expect(error.name).toBe('SchemaConfigError');
  });

  it('should have correct message', () => {
    const error = new SchemaConfigError('test message');
    expect(error.message).toBe('test message');
  });

  it('should be catchable as Error', () => {
    let caught = false;
    try {
      throw new SchemaConfigError('test');
    } catch (error) {
      if (error instanceof Error) {
        caught = true;
      }
    }
    expect(caught).toBe(true);
  });
});
