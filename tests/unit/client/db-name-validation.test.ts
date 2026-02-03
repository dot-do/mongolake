/**
 * Database Name Validation Tests
 *
 * Tests for database name extraction and validation from paths.
 * Covers security concerns like path traversal and special character handling.
 */

import { describe, it, expect } from 'vitest';
import { MongoLake, ValidationError } from '../../../src/client/index.js';
import { validateDatabaseName } from '../../../src/validation/index.js';
import { createTestClient } from './test-helpers.js';

describe('Database Name Validation', () => {
  describe('valid database names extracted correctly', () => {
    it('should accept simple alphanumeric names', () => {
      const client = createTestClient();
      const db = client.db('myapp');
      expect(db.name).toBe('myapp');
    });

    it('should accept names with underscores in the middle', () => {
      const client = createTestClient();
      const db = client.db('my_database');
      expect(db.name).toBe('my_database');
    });

    it('should accept names with hyphens in the middle', () => {
      const client = createTestClient();
      const db = client.db('my-database');
      expect(db.name).toBe('my-database');
    });

    it('should accept numeric names', () => {
      const client = createTestClient();
      const db = client.db('db123');
      expect(db.name).toBe('db123');
    });

    it('should accept mixed case names', () => {
      const client = createTestClient();
      const db = client.db('MyDatabase');
      expect(db.name).toBe('MyDatabase');
    });

    it('should accept single character names', () => {
      const client = createTestClient();
      const db = client.db('a');
      expect(db.name).toBe('a');
    });
  });

  describe('path traversal in names rejected (../)', () => {
    it('should reject parent directory traversal with forward slashes', () => {
      const client = createTestClient();
      expect(() => client.db('../etc/passwd')).toThrow(ValidationError);
    });

    it('should reject parent directory traversal with backslashes', () => {
      const client = createTestClient();
      expect(() => client.db('..\\windows\\system32')).toThrow(ValidationError);
    });

    it('should reject double dot without slashes', () => {
      const client = createTestClient();
      expect(() => client.db('..')).toThrow(ValidationError);
    });

    it('should reject embedded path traversal', () => {
      const client = createTestClient();
      expect(() => client.db('valid/../secret')).toThrow(ValidationError);
    });

    it('should reject path traversal with multiple levels', () => {
      const client = createTestClient();
      expect(() => client.db('../../root')).toThrow(ValidationError);
    });

    it('should reject forward slash paths', () => {
      const client = createTestClient();
      expect(() => client.db('path/to/db')).toThrow(ValidationError);
    });

    it('should reject backslash paths', () => {
      const client = createTestClient();
      expect(() => client.db('path\\to\\db')).toThrow(ValidationError);
    });
  });

  describe('system file paths rejected', () => {
    it('should reject absolute Unix paths', () => {
      const client = createTestClient();
      expect(() => client.db('/etc/passwd')).toThrow(ValidationError);
    });

    it('should reject Unix hidden file paths', () => {
      const client = createTestClient();
      expect(() => client.db('.bashrc')).toThrow(ValidationError);
    });

    it('should reject names starting with dot', () => {
      const client = createTestClient();
      expect(() => client.db('.hidden')).toThrow(ValidationError);
    });

    it('should reject Windows-style absolute paths', () => {
      const client = createTestClient();
      expect(() => client.db('C:\\Windows\\System32')).toThrow(ValidationError);
    });
  });

  describe('special characters handled', () => {
    it('should reject names with spaces', () => {
      const client = createTestClient();
      expect(() => client.db('my database')).toThrow(ValidationError);
    });

    it('should reject names with dollar sign', () => {
      const client = createTestClient();
      expect(() => client.db('db$name')).toThrow(ValidationError);
    });

    it('should reject names with asterisk', () => {
      const client = createTestClient();
      expect(() => client.db('db*name')).toThrow(ValidationError);
    });

    it('should reject names with question mark', () => {
      const client = createTestClient();
      expect(() => client.db('db?name')).toThrow(ValidationError);
    });

    it('should reject names with quotes', () => {
      const client = createTestClient();
      expect(() => client.db('db"name')).toThrow(ValidationError);
    });

    it('should reject names with angle brackets', () => {
      const client = createTestClient();
      expect(() => client.db('db<name>')).toThrow(ValidationError);
    });

    it('should reject names with pipe character', () => {
      const client = createTestClient();
      expect(() => client.db('db|name')).toThrow(ValidationError);
    });

    it('should reject names with colon', () => {
      const client = createTestClient();
      expect(() => client.db('db:name')).toThrow(ValidationError);
    });

    it('should reject names with null byte', () => {
      const client = createTestClient();
      expect(() => client.db('db\0name')).toThrow(ValidationError);
    });
  });

  describe('empty names rejected', () => {
    it('should reject empty string via validation function', () => {
      // Note: client.db('') falls back to default database name
      // Empty string is rejected by validateDatabaseName directly
      expect(() => validateDatabaseName('')).toThrow(ValidationError);
    });

    it('should use default database when empty string passed to db()', () => {
      // client.db('') uses falsy check and falls back to default
      const client = createTestClient();
      const db = client.db('');
      expect(db.name).toBe('default');
    });

    it('should reject whitespace-only names', () => {
      const client = createTestClient();
      expect(() => client.db('   ')).toThrow(ValidationError);
    });
  });

  describe('names with dots validated', () => {
    it('should reject names with single dot', () => {
      const client = createTestClient();
      expect(() => client.db('.')).toThrow(ValidationError);
    });

    it('should reject names with embedded dots', () => {
      const client = createTestClient();
      expect(() => client.db('my.database')).toThrow(ValidationError);
    });

    it('should reject names with dot at start', () => {
      const client = createTestClient();
      expect(() => client.db('.mydb')).toThrow(ValidationError);
    });

    it('should reject names with dot at end', () => {
      const client = createTestClient();
      expect(() => client.db('mydb.')).toThrow(ValidationError);
    });

    it('should reject names with multiple dots', () => {
      const client = createTestClient();
      expect(() => client.db('my.db.name')).toThrow(ValidationError);
    });
  });

  describe('names starting with underscore or hyphen rejected', () => {
    it('should reject names starting with underscore', () => {
      const client = createTestClient();
      expect(() => client.db('_internal')).toThrow(ValidationError);
    });

    it('should reject names starting with hyphen', () => {
      const client = createTestClient();
      expect(() => client.db('-invalid')).toThrow(ValidationError);
    });
  });

  describe('database name length validation', () => {
    it('should accept names up to maximum length', () => {
      const client = createTestClient();
      const longName = 'a'.repeat(120);
      const db = client.db(longName);
      expect(db.name).toBe(longName);
    });

    it('should reject names exceeding maximum length', () => {
      const client = createTestClient();
      const tooLongName = 'a'.repeat(121);
      expect(() => client.db(tooLongName)).toThrow(ValidationError);
    });
  });

  describe('dropDatabase validation', () => {
    it('should validate database name before dropping', async () => {
      const client = createTestClient();
      await expect(client.dropDatabase('../secret')).rejects.toThrow(ValidationError);
    });

    it('should reject path traversal in dropDatabase', async () => {
      const client = createTestClient();
      await expect(client.dropDatabase('valid/../admin')).rejects.toThrow(ValidationError);
    });

    it('should reject dots in dropDatabase name', async () => {
      const client = createTestClient();
      await expect(client.dropDatabase('..')).rejects.toThrow(ValidationError);
    });
  });

  describe('validateDatabaseName function directly', () => {
    it('should not throw for valid names', () => {
      expect(() => validateDatabaseName('validname')).not.toThrow();
      expect(() => validateDatabaseName('valid_name')).not.toThrow();
      expect(() => validateDatabaseName('valid-name')).not.toThrow();
    });

    it('should throw ValidationError with descriptive message for empty names', () => {
      try {
        validateDatabaseName('');
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ValidationError);
        expect((error as ValidationError).message).toContain('empty');
      }
    });

    it('should throw ValidationError with descriptive message for path traversal', () => {
      try {
        validateDatabaseName('../etc');
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ValidationError);
        expect((error as ValidationError).message).toMatch(/dots|slashes/i);
      }
    });

    it('should throw ValidationError for non-string input', () => {
      expect(() => validateDatabaseName(null as unknown as string)).toThrow(ValidationError);
      expect(() => validateDatabaseName(undefined as unknown as string)).toThrow(ValidationError);
      expect(() => validateDatabaseName(123 as unknown as string)).toThrow(ValidationError);
    });
  });
});
