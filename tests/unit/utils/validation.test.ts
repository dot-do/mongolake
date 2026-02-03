/**
 * Tests for name validation utilities
 *
 * These tests verify that database and collection names are properly
 * validated to prevent path traversal attacks.
 */

import { describe, it, expect } from 'vitest';
import {
  validateDatabaseName,
  validateCollectionName,
  ValidationError,
} from '../../../src/utils/validation.js';

describe('validateDatabaseName', () => {
  describe('valid names', () => {
    it('should accept simple alphanumeric names', () => {
      expect(() => validateDatabaseName('mydb')).not.toThrow();
      expect(() => validateDatabaseName('database1')).not.toThrow();
      expect(() => validateDatabaseName('DB123')).not.toThrow();
    });

    it('should accept names with underscores', () => {
      expect(() => validateDatabaseName('my_database')).not.toThrow();
      expect(() => validateDatabaseName('user_data_v2')).not.toThrow();
    });

    it('should accept names with hyphens', () => {
      expect(() => validateDatabaseName('my-database')).not.toThrow();
      expect(() => validateDatabaseName('prod-db-v1')).not.toThrow();
    });

    it('should accept mixed valid characters', () => {
      expect(() => validateDatabaseName('My_Database-123')).not.toThrow();
      expect(() => validateDatabaseName('app1_data-backup')).not.toThrow();
    });

    it('should accept single character names', () => {
      expect(() => validateDatabaseName('a')).not.toThrow();
      expect(() => validateDatabaseName('Z')).not.toThrow();
      expect(() => validateDatabaseName('1')).not.toThrow();
    });

    it('should accept names up to 120 characters', () => {
      const longName = 'a'.repeat(120);
      expect(() => validateDatabaseName(longName)).not.toThrow();
    });
  });

  describe('invalid names - empty', () => {
    it('should reject empty string', () => {
      expect(() => validateDatabaseName('')).toThrow(ValidationError);
      expect(() => validateDatabaseName('')).toThrow('database name cannot be empty');
    });
  });

  describe('invalid names - length', () => {
    it('should reject names exceeding 120 characters', () => {
      const tooLongName = 'a'.repeat(121);
      expect(() => validateDatabaseName(tooLongName)).toThrow(ValidationError);
      expect(() => validateDatabaseName(tooLongName)).toThrow('exceeds maximum length');
    });
  });

  describe('invalid names - path traversal characters', () => {
    it('should reject names with forward slashes', () => {
      expect(() => validateDatabaseName('path/to/db')).toThrow(ValidationError);
      expect(() => validateDatabaseName('../parent')).toThrow(ValidationError);
      expect(() => validateDatabaseName('/absolute')).toThrow(ValidationError);
    });

    it('should reject names with backslashes', () => {
      expect(() => validateDatabaseName('path\\to\\db')).toThrow(ValidationError);
      expect(() => validateDatabaseName('..\\parent')).toThrow(ValidationError);
    });

    it('should reject names with dots', () => {
      expect(() => validateDatabaseName('.')).toThrow(ValidationError);
      expect(() => validateDatabaseName('..')).toThrow(ValidationError);
      expect(() => validateDatabaseName('my.database')).toThrow(ValidationError);
      expect(() => validateDatabaseName('.hidden')).toThrow(ValidationError);
    });

    it('should reject names with null bytes', () => {
      expect(() => validateDatabaseName('db\0name')).toThrow(ValidationError);
      expect(() => validateDatabaseName('\0')).toThrow(ValidationError);
    });
  });

  describe('invalid names - starting characters', () => {
    it('should reject names starting with underscore', () => {
      expect(() => validateDatabaseName('_hidden')).toThrow(ValidationError);
      expect(() => validateDatabaseName('_')).toThrow(ValidationError);
    });

    it('should reject names starting with hyphen', () => {
      expect(() => validateDatabaseName('-invalid')).toThrow(ValidationError);
      expect(() => validateDatabaseName('-')).toThrow(ValidationError);
    });
  });

  describe('invalid names - invalid characters', () => {
    it('should reject names with spaces', () => {
      expect(() => validateDatabaseName('my database')).toThrow(ValidationError);
      expect(() => validateDatabaseName(' ')).toThrow(ValidationError);
    });

    it('should reject names with special characters', () => {
      expect(() => validateDatabaseName('db@name')).toThrow(ValidationError);
      expect(() => validateDatabaseName('db#name')).toThrow(ValidationError);
      expect(() => validateDatabaseName('db$name')).toThrow(ValidationError);
      expect(() => validateDatabaseName('db%name')).toThrow(ValidationError);
      expect(() => validateDatabaseName('db*name')).toThrow(ValidationError);
    });
  });
});

describe('validateCollectionName', () => {
  describe('valid names', () => {
    it('should accept simple alphanumeric names', () => {
      expect(() => validateCollectionName('users')).not.toThrow();
      expect(() => validateCollectionName('products123')).not.toThrow();
      expect(() => validateCollectionName('MyCollection')).not.toThrow();
    });

    it('should accept names with underscores', () => {
      expect(() => validateCollectionName('user_profiles')).not.toThrow();
      expect(() => validateCollectionName('order_items_v2')).not.toThrow();
    });

    it('should accept names with hyphens', () => {
      expect(() => validateCollectionName('user-profiles')).not.toThrow();
      expect(() => validateCollectionName('order-items-v1')).not.toThrow();
    });
  });

  describe('invalid names - path traversal', () => {
    it('should reject path traversal attempts', () => {
      expect(() => validateCollectionName('../etc/passwd')).toThrow(ValidationError);
      expect(() => validateCollectionName('../../secret')).toThrow(ValidationError);
      expect(() => validateCollectionName('users/../admin')).toThrow(ValidationError);
    });
  });

  describe('invalid names - special sequences', () => {
    it('should reject names that could be used for directory traversal', () => {
      expect(() => validateCollectionName('..')).toThrow(ValidationError);
      expect(() => validateCollectionName('...')).toThrow(ValidationError);
    });

    it('should reject hidden file patterns', () => {
      expect(() => validateCollectionName('.htaccess')).toThrow(ValidationError);
      expect(() => validateCollectionName('.env')).toThrow(ValidationError);
    });
  });
});

describe('ValidationError', () => {
  it('should be an instance of Error', () => {
    try {
      validateDatabaseName('');
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(ValidationError);
    }
  });

  it('should have the correct name property', () => {
    try {
      validateDatabaseName('');
    } catch (error) {
      expect((error as ValidationError).name).toBe('ValidationError');
    }
  });

  it('should have descriptive message', () => {
    try {
      validateDatabaseName('../etc/passwd');
    } catch (error) {
      expect((error as Error).message).toContain('database name');
    }
  });
});

describe('Integration with client', () => {
  // These tests verify that the validation is actually being called
  // by the MongoLake client

  it('should import validation from client module', async () => {
    const clientModule = await import('../../../src/client/index.js');
    expect(clientModule.validateDatabaseName).toBeDefined();
    expect(clientModule.validateCollectionName).toBeDefined();
    expect(clientModule.ValidationError).toBeDefined();
  });
});

// ============================================================================
// Error Scenario Tests - Invalid Input Types
// ============================================================================

describe('validateDatabaseName - Error Scenarios', () => {
  describe('invalid input types', () => {
    it('should reject null input', () => {
      expect(() => validateDatabaseName(null as unknown as string)).toThrow(ValidationError);
      expect(() => validateDatabaseName(null as unknown as string)).toThrow('cannot be empty');
    });

    it('should reject undefined input', () => {
      expect(() => validateDatabaseName(undefined as unknown as string)).toThrow(ValidationError);
      expect(() => validateDatabaseName(undefined as unknown as string)).toThrow('cannot be empty');
    });

    it('should reject number input', () => {
      expect(() => validateDatabaseName(123 as unknown as string)).toThrow(ValidationError);
      expect(() => validateDatabaseName(123 as unknown as string)).toThrow('cannot be empty');
    });

    it('should reject boolean input', () => {
      expect(() => validateDatabaseName(true as unknown as string)).toThrow(ValidationError);
      expect(() => validateDatabaseName(false as unknown as string)).toThrow(ValidationError);
    });

    it('should reject object input', () => {
      expect(() => validateDatabaseName({} as unknown as string)).toThrow(ValidationError);
      expect(() => validateDatabaseName({ name: 'test' } as unknown as string)).toThrow(ValidationError);
    });

    it('should reject array input', () => {
      expect(() => validateDatabaseName([] as unknown as string)).toThrow(ValidationError);
      expect(() => validateDatabaseName(['test'] as unknown as string)).toThrow(ValidationError);
    });

    it('should reject function input', () => {
      expect(() => validateDatabaseName((() => 'test') as unknown as string)).toThrow(ValidationError);
    });

    it('should reject symbol input', () => {
      expect(() => validateDatabaseName(Symbol('test') as unknown as string)).toThrow(ValidationError);
    });
  });

  describe('edge cases - whitespace', () => {
    it('should reject string with only whitespace', () => {
      expect(() => validateDatabaseName('   ')).toThrow(ValidationError);
      expect(() => validateDatabaseName('\t')).toThrow(ValidationError);
      expect(() => validateDatabaseName('\n')).toThrow(ValidationError);
      expect(() => validateDatabaseName('\r\n')).toThrow(ValidationError);
    });

    it('should reject string with leading whitespace', () => {
      expect(() => validateDatabaseName(' database')).toThrow(ValidationError);
      expect(() => validateDatabaseName('\tdatabase')).toThrow(ValidationError);
    });

    it('should reject string with trailing whitespace', () => {
      expect(() => validateDatabaseName('database ')).toThrow(ValidationError);
      expect(() => validateDatabaseName('database\t')).toThrow(ValidationError);
    });
  });

  describe('edge cases - unicode', () => {
    it('should reject unicode characters', () => {
      expect(() => validateDatabaseName('database\u0000')).toThrow(ValidationError);
      expect(() => validateDatabaseName('dat\u00e9base')).toThrow(ValidationError);
      expect(() => validateDatabaseName('\u0000')).toThrow(ValidationError);
    });

    it('should reject emoji', () => {
      expect(() => validateDatabaseName('database\uD83D\uDE00')).toThrow(ValidationError);
      expect(() => validateDatabaseName('\uD83D\uDE00')).toThrow(ValidationError);
    });

    it('should reject non-ASCII characters', () => {
      expect(() => validateDatabaseName('\u00e9')).toThrow(ValidationError); // e with acute accent
      expect(() => validateDatabaseName('\u4e2d\u6587')).toThrow(ValidationError); // Chinese characters
      expect(() => validateDatabaseName('\u0410\u0411\u0412')).toThrow(ValidationError); // Cyrillic
    });
  });

  describe('edge cases - control characters', () => {
    it('should reject control characters', () => {
      expect(() => validateDatabaseName('db\x00name')).toThrow(ValidationError);
      expect(() => validateDatabaseName('db\x01name')).toThrow(ValidationError);
      expect(() => validateDatabaseName('db\x1fname')).toThrow(ValidationError);
      expect(() => validateDatabaseName('db\x7fname')).toThrow(ValidationError); // DEL character
    });

    it('should reject carriage return and line feed', () => {
      expect(() => validateDatabaseName('db\rname')).toThrow(ValidationError);
      expect(() => validateDatabaseName('db\nname')).toThrow(ValidationError);
      expect(() => validateDatabaseName('db\r\nname')).toThrow(ValidationError);
    });
  });

  describe('edge cases - boundary testing', () => {
    it('should accept name at exactly max length', () => {
      const maxLengthName = 'a'.repeat(120);
      expect(() => validateDatabaseName(maxLengthName)).not.toThrow();
    });

    it('should reject name at max length + 1', () => {
      const tooLongName = 'a'.repeat(121);
      expect(() => validateDatabaseName(tooLongName)).toThrow(ValidationError);
      expect(() => validateDatabaseName(tooLongName)).toThrow('exceeds maximum length');
    });

    it('should reject very long names (stress test)', () => {
      const veryLongName = 'a'.repeat(10000);
      expect(() => validateDatabaseName(veryLongName)).toThrow(ValidationError);
    });
  });

  describe('edge cases - shell injection attempts', () => {
    it('should reject shell metacharacters', () => {
      expect(() => validateDatabaseName('db;rm -rf /')).toThrow(ValidationError);
      expect(() => validateDatabaseName('db`whoami`')).toThrow(ValidationError);
      expect(() => validateDatabaseName('db$(whoami)')).toThrow(ValidationError);
      expect(() => validateDatabaseName('db|cat /etc/passwd')).toThrow(ValidationError);
      expect(() => validateDatabaseName('db&&echo')).toThrow(ValidationError);
    });

    it('should reject SQL injection patterns', () => {
      expect(() => validateDatabaseName("db'; DROP TABLE--")).toThrow(ValidationError);
      expect(() => validateDatabaseName('db" OR "1"="1')).toThrow(ValidationError);
    });
  });
});

describe('validateCollectionName - Error Scenarios', () => {
  describe('invalid input types', () => {
    it('should reject null input', () => {
      expect(() => validateCollectionName(null as unknown as string)).toThrow(ValidationError);
    });

    it('should reject undefined input', () => {
      expect(() => validateCollectionName(undefined as unknown as string)).toThrow(ValidationError);
    });

    it('should reject number input', () => {
      expect(() => validateCollectionName(42 as unknown as string)).toThrow(ValidationError);
    });

    it('should reject object input', () => {
      expect(() => validateCollectionName({} as unknown as string)).toThrow(ValidationError);
    });
  });

  describe('edge cases - system files', () => {
    it('should reject names that could target system files', () => {
      expect(() => validateCollectionName('.bashrc')).toThrow(ValidationError);
      expect(() => validateCollectionName('.profile')).toThrow(ValidationError);
      expect(() => validateCollectionName('.ssh/id_rsa')).toThrow(ValidationError);
    });

    it('should reject names with double extensions', () => {
      expect(() => validateCollectionName('file.tar.gz')).toThrow(ValidationError);
      expect(() => validateCollectionName('backup.sql.bak')).toThrow(ValidationError);
    });
  });

  describe('edge cases - reserved names', () => {
    it('should not accept common reserved filesystem names', () => {
      // These contain dots so they should be rejected
      expect(() => validateCollectionName('.')).toThrow(ValidationError);
      expect(() => validateCollectionName('..')).toThrow(ValidationError);
    });
  });
});

describe('ValidationError - Error Scenarios', () => {
  describe('error properties', () => {
    it('should be catchable as Error', () => {
      let caught = false;
      try {
        validateDatabaseName('');
      } catch (error) {
        if (error instanceof Error) {
          caught = true;
        }
      }
      expect(caught).toBe(true);
    });

    it('should be catchable as ValidationError', () => {
      let caught = false;
      try {
        validateDatabaseName('');
      } catch (error) {
        if (error instanceof ValidationError) {
          caught = true;
        }
      }
      expect(caught).toBe(true);
    });

    it('should have stack trace', () => {
      try {
        validateDatabaseName('');
      } catch (error) {
        expect((error as Error).stack).toBeDefined();
        expect((error as Error).stack).toContain('validateDatabaseName');
      }
    });

    it('should preserve error name across JSON serialization', () => {
      try {
        validateDatabaseName('');
      } catch (error) {
        const serialized = JSON.stringify({
          name: (error as Error).name,
          message: (error as Error).message,
        });
        const parsed = JSON.parse(serialized);
        expect(parsed.name).toBe('ValidationError');
      }
    });
  });

  describe('error messages - descriptive', () => {
    it('should include the type of name in error message', () => {
      try {
        validateDatabaseName('../etc/passwd');
      } catch (error) {
        expect((error as Error).message).toContain('database');
      }

      try {
        validateCollectionName('../etc/passwd');
      } catch (error) {
        expect((error as Error).message).toContain('collection');
      }
    });

    it('should provide specific error for each validation failure type', () => {
      // Empty name
      try {
        validateDatabaseName('');
      } catch (error) {
        expect((error as Error).message).toContain('empty');
      }

      // Too long
      try {
        validateDatabaseName('a'.repeat(200));
      } catch (error) {
        expect((error as Error).message).toContain('maximum length');
      }

      // Contains dot
      try {
        validateDatabaseName('my.db');
      } catch (error) {
        expect((error as Error).message).toContain('dots');
      }

      // Contains slash
      try {
        validateDatabaseName('my/db');
      } catch (error) {
        expect((error as Error).message).toContain('slashes');
      }

      // Starts with underscore
      try {
        validateDatabaseName('_mydb');
      } catch (error) {
        expect((error as Error).message).toContain('underscore');
      }
    });
  });
});
