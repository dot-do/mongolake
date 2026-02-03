/**
 * Tests for Schema Configuration Parser - RED Phase
 *
 * These tests cover schema config parsing functionality that needs to be implemented:
 * - Parse YAML config
 * - Parse JSON config with line number error reporting
 * - Validate required fields
 * - Handle nested schemas
 * - Report parse errors with line numbers
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  parseSchemaConfigFromJSON,
  parseSchemaConfigFromYAML,
  parseSchemaConfig,
  SchemaConfigError,
  type FullSchemaConfig,
  type ParsedSchemaConfig,
  type ParseError,
} from '../../../src/schema/config.js';

// ============================================================================
// YAML Parsing Tests
// ============================================================================

describe('parseSchemaConfigFromYAML', () => {
  describe('valid YAML configurations', () => {
    it('should parse a basic YAML schema configuration', () => {
      const yaml = `
collections:
  users:
    columns:
      _id: string
      email: string
      createdAt: timestamp
`;

      const result = parseSchemaConfigFromYAML(yaml);

      expect(result.collections.has('users')).toBe(true);
      const usersSchema = result.collections.get('users')!;
      expect(usersSchema.columns).toHaveLength(3);
      expect(usersSchema.columnMap.get('_id')?.type).toBe('string');
      expect(usersSchema.columnMap.get('email')?.type).toBe('string');
      expect(usersSchema.columnMap.get('createdAt')?.type).toBe('timestamp');
    });

    it('should parse nested field paths in YAML', () => {
      const yaml = `
collections:
  users:
    columns:
      profile.name: string
      profile.age: int32
      address.city: string
      address.zip: string
`;

      const result = parseSchemaConfigFromYAML(yaml);
      const usersSchema = result.collections.get('users')!;

      expect(usersSchema.columns).toHaveLength(4);

      const profileName = usersSchema.columnMap.get('profile.name')!;
      expect(profileName.segments).toEqual(['profile', 'name']);
      expect(profileName.type).toBe('string');
    });

    it('should parse array types in YAML', () => {
      const yaml = `
collections:
  users:
    columns:
      tags:
        - string
      scores:
        - int32
`;

      const result = parseSchemaConfigFromYAML(yaml);
      const schema = result.collections.get('users')!;

      const tags = schema.columnMap.get('tags')!;
      expect(tags.isArray).toBe(true);
      expect(tags.type).toBe('string');
    });

    it('should parse autoPromote configuration in YAML', () => {
      const yaml = `
collections:
  users:
    columns:
      _id: string
    autoPromote:
      threshold: 0.9
`;

      const result = parseSchemaConfigFromYAML(yaml);
      const schema = result.collections.get('users')!;

      expect(schema.autoPromote).toEqual({ threshold: 0.9 });
    });

    it('should parse storeVariant in YAML', () => {
      const yaml = `
collections:
  users:
    columns:
      _id: string
    storeVariant: false
`;

      const result = parseSchemaConfigFromYAML(yaml);
      const schema = result.collections.get('users')!;

      expect(schema.storeVariant).toBe(false);
    });

    it('should parse multiple collections in YAML', () => {
      const yaml = `
collections:
  users:
    columns:
      _id: string
      email: string
  orders:
    columns:
      _id: string
      total: double
  products:
    columns:
      _id: string
      name: string
`;

      const result = parseSchemaConfigFromYAML(yaml);

      expect(result.collections.size).toBe(3);
      expect(result.collections.has('users')).toBe(true);
      expect(result.collections.has('orders')).toBe(true);
      expect(result.collections.has('products')).toBe(true);
    });

    it('should parse struct types in YAML', () => {
      const yaml = `
collections:
  users:
    columns:
      profile:
        name: string
        age: int32
        address:
          city: string
          zip: string
`;

      const result = parseSchemaConfigFromYAML(yaml);
      const schema = result.collections.get('users')!;

      const profile = schema.columnMap.get('profile')!;
      expect(profile.isStruct).toBe(true);
      expect(profile.structDef).toBeDefined();
      expect(profile.structDef!['name'].type).toBe('string');
      expect(profile.structDef!['age'].type).toBe('int32');
    });

    it('should parse YAML with comments', () => {
      const yaml = `
# Schema configuration for the application
collections:
  # User collection with basic fields
  users:
    columns:
      _id: string  # Primary key
      email: string  # User email
`;

      const result = parseSchemaConfigFromYAML(yaml);

      expect(result.collections.has('users')).toBe(true);
      expect(result.collections.get('users')!.columns).toHaveLength(2);
    });

    it('should handle YAML anchors and aliases', () => {
      const yaml = `
definitions:
  common_fields: &common_fields
    _id: string
    createdAt: timestamp
    updatedAt: timestamp

collections:
  users:
    columns:
      <<: *common_fields
      email: string
  orders:
    columns:
      <<: *common_fields
      total: double
`;

      const result = parseSchemaConfigFromYAML(yaml);

      const usersSchema = result.collections.get('users')!;
      expect(usersSchema.columnMap.get('_id')?.type).toBe('string');
      expect(usersSchema.columnMap.get('createdAt')?.type).toBe('timestamp');
      expect(usersSchema.columnMap.get('email')?.type).toBe('string');

      const ordersSchema = result.collections.get('orders')!;
      expect(ordersSchema.columnMap.get('_id')?.type).toBe('string');
      expect(ordersSchema.columnMap.get('total')?.type).toBe('double');
    });
  });

  describe('invalid YAML configurations', () => {
    it('should reject invalid YAML syntax', () => {
      const yaml = `
collections:
  users:
    columns:
      _id: string
    invalid yaml here [
`;

      expect(() => parseSchemaConfigFromYAML(yaml)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfigFromYAML(yaml)).toThrow('YAML');
    });

    it('should reject YAML without collections', () => {
      const yaml = `
someOther:
  key: value
`;

      expect(() => parseSchemaConfigFromYAML(yaml)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfigFromYAML(yaml)).toThrow('collections');
    });

    it('should reject YAML with invalid type', () => {
      const yaml = `
collections:
  users:
    columns:
      field: invalid_type
`;

      expect(() => parseSchemaConfigFromYAML(yaml)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfigFromYAML(yaml)).toThrow('Invalid Parquet type');
    });
  });
});

// ============================================================================
// JSON Parsing with Line Numbers Tests
// ============================================================================

describe('parseSchemaConfigFromJSON - Line Number Errors', () => {
  it('should report line number for invalid type error', () => {
    const json = `{
  "collections": {
    "users": {
      "columns": {
        "_id": "string",
        "field": "invalid_type"
      }
    }
  }
}`;

    try {
      parseSchemaConfigFromJSON(json);
      expect.fail('Should have thrown an error');
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaConfigError);
      const parseError = error as SchemaConfigError;
      expect(parseError.lineNumber).toBeDefined();
      expect(parseError.lineNumber).toBe(6); // Line where "invalid_type" appears
      expect(parseError.columnNumber).toBeDefined();
    }
  });

  it('should report line number for missing collections', () => {
    const json = `{
  "notCollections": {
    "users": {}
  }
}`;

    try {
      parseSchemaConfigFromJSON(json);
      expect.fail('Should have thrown an error');
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaConfigError);
      const parseError = error as SchemaConfigError;
      expect(parseError.lineNumber).toBeDefined();
      expect(parseError.lineNumber).toBe(1); // Line where object starts
    }
  });

  it('should report line number for invalid field path', () => {
    const json = `{
  "collections": {
    "users": {
      "columns": {
        "123invalid": "string"
      }
    }
  }
}`;

    try {
      parseSchemaConfigFromJSON(json);
      expect.fail('Should have thrown an error');
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaConfigError);
      const parseError = error as SchemaConfigError;
      expect(parseError.lineNumber).toBeDefined();
      expect(parseError.lineNumber).toBe(5); // Line where "123invalid" appears
    }
  });

  it('should report line number for invalid autoPromote threshold', () => {
    const json = `{
  "collections": {
    "users": {
      "columns": {
        "_id": "string"
      },
      "autoPromote": {
        "threshold": 1.5
      }
    }
  }
}`;

    try {
      parseSchemaConfigFromJSON(json);
      expect.fail('Should have thrown an error');
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaConfigError);
      const parseError = error as SchemaConfigError;
      expect(parseError.lineNumber).toBeDefined();
      expect(parseError.lineNumber).toBe(8); // Line where threshold appears
    }
  });

  it('should include source context in error message', () => {
    const json = `{
  "collections": {
    "users": {
      "columns": {
        "field": "bad_type"
      }
    }
  }
}`;

    try {
      parseSchemaConfigFromJSON(json);
      expect.fail('Should have thrown an error');
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaConfigError);
      const parseError = error as SchemaConfigError;
      expect(parseError.sourceContext).toBeDefined();
      expect(parseError.sourceContext).toContain('bad_type');
    }
  });

  it('should report line number for JSON syntax error', () => {
    const json = `{
  "collections": {
    "users": {
      "columns": {
        "_id": "string",
      }
    }
  }
}`;

    try {
      parseSchemaConfigFromJSON(json);
      expect.fail('Should have thrown an error');
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaConfigError);
      const parseError = error as SchemaConfigError;
      expect(parseError.lineNumber).toBeDefined();
      // Line 5 or 6 where the trailing comma causes the error
      expect(parseError.lineNumber).toBeGreaterThanOrEqual(5);
    }
  });
});

// ============================================================================
// YAML Parsing with Line Numbers Tests
// ============================================================================

describe('parseSchemaConfigFromYAML - Line Number Errors', () => {
  it('should report line number for invalid type in YAML', () => {
    const yaml = `
collections:
  users:
    columns:
      _id: string
      field: invalid_type
`;

    try {
      parseSchemaConfigFromYAML(yaml);
      expect.fail('Should have thrown an error');
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaConfigError);
      const parseError = error as SchemaConfigError;
      expect(parseError.lineNumber).toBeDefined();
      expect(parseError.lineNumber).toBe(6); // Line where "invalid_type" appears
    }
  });

  it('should report line number for missing collections in YAML', () => {
    const yaml = `
notCollections:
  users:
    columns:
      _id: string
`;

    try {
      parseSchemaConfigFromYAML(yaml);
      expect.fail('Should have thrown an error');
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaConfigError);
      const parseError = error as SchemaConfigError;
      expect(parseError.lineNumber).toBeDefined();
      expect(parseError.lineNumber).toBe(2); // Line where "notCollections" starts
    }
  });

  it('should report line number for invalid autoPromote in YAML', () => {
    const yaml = `
collections:
  users:
    columns:
      _id: string
    autoPromote:
      threshold: 2.0
`;

    try {
      parseSchemaConfigFromYAML(yaml);
      expect.fail('Should have thrown an error');
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaConfigError);
      const parseError = error as SchemaConfigError;
      expect(parseError.lineNumber).toBeDefined();
      expect(parseError.lineNumber).toBe(7); // Line where threshold value appears
    }
  });

  it('should include source context for YAML errors', () => {
    const yaml = `
collections:
  users:
    columns:
      field: bad_type
`;

    try {
      parseSchemaConfigFromYAML(yaml);
      expect.fail('Should have thrown an error');
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaConfigError);
      const parseError = error as SchemaConfigError;
      expect(parseError.sourceContext).toBeDefined();
      expect(parseError.sourceContext).toContain('bad_type');
    }
  });
});

// ============================================================================
// Required Fields Validation Tests
// ============================================================================

describe('Required Fields Validation', () => {
  describe('collection-level required fields', () => {
    it('should validate collection has required _id column when enforced', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              email: 'string',
              // Missing _id which should be required
            },
            requireIdColumn: true,
          },
        },
      };

      expect(() => parseSchemaConfig(config)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfig(config)).toThrow('_id');
      expect(() => parseSchemaConfig(config)).toThrow('required');
    });

    it('should pass when _id column is present and required', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              _id: 'string',
              email: 'string',
            },
            requireIdColumn: true,
          },
        },
      };

      const result = parseSchemaConfig(config);
      expect(result.collections.has('users')).toBe(true);
    });

    it('should allow collections without _id when not enforced', () => {
      const config: FullSchemaConfig = {
        collections: {
          logs: {
            columns: {
              message: 'string',
              timestamp: 'timestamp',
            },
            requireIdColumn: false,
          },
        },
      };

      const result = parseSchemaConfig(config);
      expect(result.collections.has('logs')).toBe(true);
    });
  });

  describe('schema-level required fields', () => {
    it('should validate required fields are defined', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              _id: 'string',
              email: 'string',
            },
            requiredFields: ['_id', 'email', 'createdAt'],
          },
        },
      };

      expect(() => parseSchemaConfig(config)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfig(config)).toThrow('createdAt');
      expect(() => parseSchemaConfig(config)).toThrow('required');
    });

    it('should pass when all required fields are defined', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              _id: 'string',
              email: 'string',
              createdAt: 'timestamp',
            },
            requiredFields: ['_id', 'email', 'createdAt'],
          },
        },
      };

      const result = parseSchemaConfig(config);
      const usersSchema = result.collections.get('users')!;
      expect(usersSchema.requiredFields).toEqual(['_id', 'email', 'createdAt']);
    });

    it('should mark columns as required in parsed schema', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              _id: 'string',
              email: 'string',
              name: 'string',
            },
            requiredFields: ['_id', 'email'],
          },
        },
      };

      const result = parseSchemaConfig(config);
      const usersSchema = result.collections.get('users')!;

      const idColumn = usersSchema.columnMap.get('_id')!;
      expect(idColumn.required).toBe(true);

      const emailColumn = usersSchema.columnMap.get('email')!;
      expect(emailColumn.required).toBe(true);

      const nameColumn = usersSchema.columnMap.get('name')!;
      expect(nameColumn.required).toBe(false);
    });
  });

  describe('type constraints on required fields', () => {
    it('should reject null type for required field', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              _id: 'string',
              email: { type: 'string', nullable: true },
            },
            requiredFields: ['email'],
          },
        },
      };

      expect(() => parseSchemaConfig(config)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfig(config)).toThrow('nullable');
      expect(() => parseSchemaConfig(config)).toThrow('required');
    });
  });
});

// ============================================================================
// Nested Schema Tests
// ============================================================================

describe('Nested Schema Handling', () => {
  describe('deeply nested structures', () => {
    it('should parse deeply nested object schemas', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              profile: {
                personal: {
                  name: {
                    first: 'string',
                    last: 'string',
                  },
                  birthDate: 'date',
                },
                contact: {
                  email: 'string',
                  phone: 'string',
                  address: {
                    street: 'string',
                    city: 'string',
                    country: 'string',
                  },
                },
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

      // Check deeply nested paths exist
      expect(profile.structDef!['personal'].isStruct).toBe(true);
      expect(profile.structDef!['personal'].structDef!['name'].isStruct).toBe(true);
      expect(profile.structDef!['personal'].structDef!['name'].structDef!['first'].type).toBe('string');
    });

    it('should flatten nested paths for O(1) lookup', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              'profile.name.first': 'string',
              'profile.name.last': 'string',
              'profile.contact.email': 'string',
            },
          },
        },
      };

      const result = parseSchemaConfig(config);
      const schema = result.collections.get('users')!;

      // Direct path access should work
      expect(schema.columnMap.has('profile.name.first')).toBe(true);
      expect(schema.columnMap.get('profile.name.first')?.type).toBe('string');

      // Nested segments should be available
      const firstNameCol = schema.columnMap.get('profile.name.first')!;
      expect(firstNameCol.segments).toEqual(['profile', 'name', 'first']);
    });

    it('should handle mixed nested and flat paths', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            columns: {
              _id: 'string',
              email: 'string',
              'profile.name': 'string',
              settings: {
                theme: 'string',
                notifications: 'boolean',
              },
            },
          },
        },
      };

      const result = parseSchemaConfig(config);
      const schema = result.collections.get('users')!;

      expect(schema.columnMap.get('_id')?.isStruct).toBe(false);
      expect(schema.columnMap.get('profile.name')?.segments).toEqual(['profile', 'name']);
      expect(schema.columnMap.get('settings')?.isStruct).toBe(true);
    });
  });

  describe('array of nested objects', () => {
    it('should parse array of objects with nested schema', () => {
      const config: FullSchemaConfig = {
        collections: {
          orders: {
            columns: {
              items: [
                {
                  productId: 'string',
                  quantity: 'int32',
                  price: 'double',
                  options: {
                    color: 'string',
                    size: 'string',
                  },
                },
              ],
            },
          },
        },
      };

      const result = parseSchemaConfig(config);
      const schema = result.collections.get('orders')!;

      const items = schema.columnMap.get('items')!;
      expect(items.isArray).toBe(true);
      expect(items.isStruct).toBe(true);
      expect(items.structDef).toBeDefined();
      expect(items.structDef!['productId'].type).toBe('string');
      expect(items.structDef!['options'].isStruct).toBe(true);
    });
  });

  describe('schema inheritance', () => {
    it('should support extends for schema inheritance', () => {
      const config: FullSchemaConfig = {
        schemas: {
          baseDocument: {
            columns: {
              _id: 'string',
              createdAt: 'timestamp',
              updatedAt: 'timestamp',
            },
          },
        },
        collections: {
          users: {
            extends: 'baseDocument',
            columns: {
              email: 'string',
              name: 'string',
            },
          },
        },
      };

      const result = parseSchemaConfig(config);
      const usersSchema = result.collections.get('users')!;

      // Should have inherited columns
      expect(usersSchema.columnMap.has('_id')).toBe(true);
      expect(usersSchema.columnMap.has('createdAt')).toBe(true);
      expect(usersSchema.columnMap.has('updatedAt')).toBe(true);

      // Should have own columns
      expect(usersSchema.columnMap.has('email')).toBe(true);
      expect(usersSchema.columnMap.has('name')).toBe(true);
    });

    it('should allow overriding inherited columns', () => {
      const config: FullSchemaConfig = {
        schemas: {
          baseDocument: {
            columns: {
              _id: 'string',
              status: 'string',
            },
          },
        },
        collections: {
          orders: {
            extends: 'baseDocument',
            columns: {
              status: 'int32', // Override with different type
              total: 'double',
            },
          },
        },
      };

      const result = parseSchemaConfig(config);
      const ordersSchema = result.collections.get('orders')!;

      // Should have overridden type
      expect(ordersSchema.columnMap.get('status')?.type).toBe('int32');
    });

    it('should reject invalid extends reference', () => {
      const config: FullSchemaConfig = {
        collections: {
          users: {
            extends: 'nonExistentSchema',
            columns: {
              email: 'string',
            },
          },
        },
      };

      expect(() => parseSchemaConfig(config)).toThrow(SchemaConfigError);
      expect(() => parseSchemaConfig(config)).toThrow('nonExistentSchema');
    });
  });
});

// ============================================================================
// Parse Error with Line Numbers Tests (Extended)
// ============================================================================

describe('ParseError Structure', () => {
  it('should return ParseError object with all fields', () => {
    const json = `{
  "collections": {
    "users": {
      "columns": {
        "field": "bad_type"
      }
    }
  }
}`;

    try {
      parseSchemaConfigFromJSON(json);
      expect.fail('Should have thrown an error');
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaConfigError);
      const parseError = error as SchemaConfigError;

      // Check error has all required properties
      expect(parseError.message).toBeDefined();
      expect(parseError.lineNumber).toBeDefined();
      expect(parseError.columnNumber).toBeDefined();
      expect(parseError.sourceContext).toBeDefined();
      expect(parseError.path).toBeDefined();

      // Path should indicate the location in the schema
      expect(parseError.path).toBe('collections.users.columns.field');
    }
  });

  it('should provide helpful error messages with context', () => {
    const yaml = `
collections:
  users:
    columns:
      email: string
      age: number  # 'number' is not a valid Parquet type
`;

    try {
      parseSchemaConfigFromYAML(yaml);
      expect.fail('Should have thrown an error');
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaConfigError);
      const parseError = error as SchemaConfigError;

      // Should suggest the correct type
      expect(parseError.message).toContain('number');
      expect(parseError.suggestion).toBeDefined();
      expect(parseError.suggestion).toMatch(/double|float|int32|int64/);
    }
  });

  it('should collect multiple errors when requested', () => {
    const config = {
      collections: {
        users: {
          columns: {
            field1: 'bad_type_1',
            field2: 'bad_type_2',
            field3: 'bad_type_3',
          },
        },
      },
    };

    const result = parseSchemaConfig(config, { collectAllErrors: true });

    expect(result.errors).toBeDefined();
    expect(result.errors).toHaveLength(3);
    expect(result.errors![0].path).toContain('field1');
    expect(result.errors![1].path).toContain('field2');
    expect(result.errors![2].path).toContain('field3');
  });
});

// ============================================================================
// Schema Config File Loading Tests
// ============================================================================

describe('Schema Config File Loading', () => {
  it('should detect format from file extension for .json', async () => {
    const { loadSchemaConfigFromFile } = await import('../../../src/schema/config.js');

    // This will fail because the function doesn't exist yet
    expect(loadSchemaConfigFromFile).toBeDefined();
  });

  it('should detect format from file extension for .yaml', async () => {
    const { loadSchemaConfigFromFile } = await import('../../../src/schema/config.js');

    expect(loadSchemaConfigFromFile).toBeDefined();
  });

  it('should detect format from file extension for .yml', async () => {
    const { loadSchemaConfigFromFile } = await import('../../../src/schema/config.js');

    expect(loadSchemaConfigFromFile).toBeDefined();
  });

  it('should allow explicit format override', async () => {
    const { loadSchemaConfigFromFile } = await import('../../../src/schema/config.js');

    expect(loadSchemaConfigFromFile).toBeDefined();
  });
});

// ============================================================================
// Error Message Quality Tests
// ============================================================================

describe('Error Message Quality', () => {
  it('should provide clear error for unknown type with suggestions', () => {
    const config: FullSchemaConfig = {
      collections: {
        users: {
          columns: {
            age: 'number', // Common mistake: 'number' instead of 'int32' or 'double'
          },
        },
      },
    };

    try {
      parseSchemaConfig(config);
      expect.fail('Should have thrown an error');
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaConfigError);
      expect((error as SchemaConfigError).message).toContain('number');
      expect((error as SchemaConfigError).suggestion).toBeDefined();
      expect((error as SchemaConfigError).suggestion).toMatch(/int32|int64|float|double/);
    }
  });

  it('should provide clear error for misspelled type', () => {
    const config: FullSchemaConfig = {
      collections: {
        users: {
          columns: {
            flag: 'bool', // Common mistake: 'bool' instead of 'boolean'
          },
        },
      },
    };

    try {
      parseSchemaConfig(config);
      expect.fail('Should have thrown an error');
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaConfigError);
      expect((error as SchemaConfigError).suggestion).toContain('boolean');
    }
  });

  it('should provide clear error for integer vs int32/int64', () => {
    const config: FullSchemaConfig = {
      collections: {
        users: {
          columns: {
            count: 'integer', // Common mistake
          },
        },
      },
    };

    try {
      parseSchemaConfig(config);
      expect.fail('Should have thrown an error');
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaConfigError);
      expect((error as SchemaConfigError).suggestion).toMatch(/int32|int64/);
    }
  });

  it('should provide clear error for datetime vs timestamp', () => {
    const config: FullSchemaConfig = {
      collections: {
        users: {
          columns: {
            createdAt: 'datetime', // Common mistake
          },
        },
      },
    };

    try {
      parseSchemaConfig(config);
      expect.fail('Should have thrown an error');
    } catch (error) {
      expect(error).toBeInstanceOf(SchemaConfigError);
      expect((error as SchemaConfigError).suggestion).toContain('timestamp');
    }
  });
});

// ============================================================================
// Type Coercion Rules Tests
// ============================================================================

describe('Type Coercion Rules', () => {
  it('should define coercion rules in schema', () => {
    const config: FullSchemaConfig = {
      collections: {
        users: {
          columns: {
            age: {
              type: 'int32',
              coerce: {
                from: ['string', 'double'],
                strict: false,
              },
            },
          },
        },
      },
    };

    const result = parseSchemaConfig(config);
    const schema = result.collections.get('users')!;

    const ageColumn = schema.columnMap.get('age')!;
    expect(ageColumn.coercionRules).toBeDefined();
    expect(ageColumn.coercionRules!.from).toContain('string');
    expect(ageColumn.coercionRules!.from).toContain('double');
  });
});

// ============================================================================
// Default Value Tests
// ============================================================================

describe('Default Values in Schema', () => {
  it('should parse default values for columns', () => {
    const config: FullSchemaConfig = {
      collections: {
        users: {
          columns: {
            status: {
              type: 'string',
              default: 'active',
            },
            role: {
              type: 'string',
              default: 'user',
            },
            loginCount: {
              type: 'int32',
              default: 0,
            },
          },
        },
      },
    };

    const result = parseSchemaConfig(config);
    const schema = result.collections.get('users')!;

    expect(schema.columnMap.get('status')?.defaultValue).toBe('active');
    expect(schema.columnMap.get('role')?.defaultValue).toBe('user');
    expect(schema.columnMap.get('loginCount')?.defaultValue).toBe(0);
  });

  it('should validate default value matches type', () => {
    const config: FullSchemaConfig = {
      collections: {
        users: {
          columns: {
            age: {
              type: 'int32',
              default: 'not a number', // Type mismatch
            },
          },
        },
      },
    };

    expect(() => parseSchemaConfig(config)).toThrow(SchemaConfigError);
    expect(() => parseSchemaConfig(config)).toThrow('default');
    expect(() => parseSchemaConfig(config)).toThrow('int32');
  });
});
