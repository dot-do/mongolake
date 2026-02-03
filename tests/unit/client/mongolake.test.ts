/**
 * MongoLake Client Tests
 *
 * Tests for the main MongoLake client class including:
 * - Client construction
 * - Database access (db method)
 * - Database listing and dropping
 * - Factory functions
 */

import { describe, it, expect } from 'vitest';
import {
  MongoLake,
  Database,
  createClient,
  createDatabase,
  ValidationError,
} from '../../../src/client/index.js';
import { MemoryStorage } from '../../../src/storage/index.js';
import { createTestClient } from './test-helpers.js';

describe('MongoLake', () => {
  describe('constructor', () => {
    it('should create client with default config', () => {
      const client = new MongoLake();
      expect(client).toBeInstanceOf(MongoLake);
    });

    it('should create client with local storage path', () => {
      const client = new MongoLake({ local: '.mongolake-test' });
      expect(client).toBeInstanceOf(MongoLake);
    });

    it('should create client with database name', () => {
      const client = new MongoLake({ database: 'myapp' });
      const database = client.db();
      expect(database.name).toBe('myapp');
    });
  });

  describe('db()', () => {
    it('should return Database instance', () => {
      const client = new MongoLake();
      const database = client.db('test');
      expect(database).toBeInstanceOf(Database);
      expect(database.name).toBe('test');
    });

    it('should return default database when no name provided', () => {
      const client = new MongoLake({ database: 'mydefault' });
      const database = client.db();
      expect(database.name).toBe('mydefault');
    });

    it('should return "default" when no database configured', () => {
      const client = new MongoLake({});
      const database = client.db();
      expect(database.name).toBe('default');
    });

    it('should cache and reuse Database instances', () => {
      const client = new MongoLake();
      const db1 = client.db('test');
      const db2 = client.db('test');
      expect(db1).toBe(db2);
    });

    it('should create separate instances for different databases', () => {
      const client = new MongoLake();
      const db1 = client.db('test1');
      const db2 = client.db('test2');
      expect(db1).not.toBe(db2);
      expect(db1.name).toBe('test1');
      expect(db2.name).toBe('test2');
    });

    it('should reject invalid database names with path traversal', () => {
      const client = new MongoLake();
      expect(() => client.db('../etc/passwd')).toThrow(ValidationError);
      expect(() => client.db('..\\windows\\system32')).toThrow(ValidationError);
      expect(() => client.db('db/collection')).toThrow(ValidationError);
    });

    it('should reject database names with dots', () => {
      const client = new MongoLake();
      expect(() => client.db('..')).toThrow(ValidationError);
      expect(() => client.db('my.database')).toThrow(ValidationError);
      expect(() => client.db('.hidden')).toThrow(ValidationError);
    });

    it('should use default database when empty string is passed', () => {
      const client = new MongoLake({ database: 'mydefault' });
      const database = client.db('');
      expect(database.name).toBe('mydefault');
    });

    it('should reject database names starting with underscore or hyphen', () => {
      const client = new MongoLake();
      expect(() => client.db('_internal')).toThrow(ValidationError);
      expect(() => client.db('-invalid')).toThrow(ValidationError);
    });
  });

  describe('listDatabases()', () => {
    it('should return empty array for fresh storage', async () => {
      const client = createTestClient();
      const databases = await client.listDatabases();
      expect(databases).toEqual([]);
    });

    it('should list databases after data is written', async () => {
      const client = createTestClient();
      // @ts-expect-error - accessing private storage for testing
      const storage = client.storage as MemoryStorage;

      // Simulate database files
      await storage.put('db1/collection1.parquet', new Uint8Array([1, 2, 3]));
      await storage.put('db2/collection2.parquet', new Uint8Array([4, 5, 6]));

      const databases = await client.listDatabases();
      expect(databases).toContain('db1');
      expect(databases).toContain('db2');
    });
  });

  describe('dropDatabase()', () => {
    it('should remove all files for a database', async () => {
      const client = createTestClient();
      // @ts-expect-error - accessing private storage for testing
      const storage = client.storage as MemoryStorage;

      await storage.put('mydb/users.parquet', new Uint8Array([1, 2, 3]));
      await storage.put('mydb/posts.parquet', new Uint8Array([4, 5, 6]));
      await storage.put('otherdb/data.parquet', new Uint8Array([7, 8, 9]));

      await client.dropDatabase('mydb');

      const files = await storage.list('');
      expect(files).not.toContain('mydb/users.parquet');
      expect(files).not.toContain('mydb/posts.parquet');
      expect(files).toContain('otherdb/data.parquet');
    });

    it('should remove cached database instance', async () => {
      const client = createTestClient();
      const db1 = client.db('mydb');
      await client.dropDatabase('mydb');
      const db2 = client.db('mydb');
      expect(db1).not.toBe(db2);
    });

    it('should reject invalid database names with path traversal', async () => {
      const client = createTestClient();
      await expect(client.dropDatabase('../etc/passwd')).rejects.toThrow(ValidationError);
      await expect(client.dropDatabase('..\\secret')).rejects.toThrow(ValidationError);
      await expect(client.dropDatabase('..')).rejects.toThrow(ValidationError);
    });
  });

  describe('close()', () => {
    it('should clear cached databases', async () => {
      const client = new MongoLake();
      client.db('test1');
      client.db('test2');
      await client.close();

      // After close, getting db should create new instances
      const newDb = client.db('test1');
      expect(newDb).toBeInstanceOf(Database);
    });
  });
});

describe('createClient() factory function', () => {
  it('should return MongoLake instance', () => {
    const client = createClient();
    expect(client).toBeInstanceOf(MongoLake);
  });

  it('should create new client each time (no singleton)', () => {
    const client1 = createClient();
    const client2 = createClient();
    expect(client1).not.toBe(client2);
  });

  it('should accept configuration options', () => {
    const client = createClient({ database: 'mydb', local: '.test' });
    expect(client).toBeInstanceOf(MongoLake);
    expect(client.db().name).toBe('mydb');
  });
});

describe('createDatabase() factory function', () => {
  it('should return Database instance', () => {
    const database = createDatabase('test');
    expect(database).toBeInstanceOf(Database);
  });

  it('should create new client each time (no singleton)', () => {
    const db1 = createDatabase('test');
    const db2 = createDatabase('test');
    expect(db1).not.toBe(db2);
  });

  it('should return default database when no name provided', () => {
    const database = createDatabase();
    expect(database).toBeInstanceOf(Database);
    expect(database.name).toBe('default');
  });

  it('should accept configuration options', () => {
    const database = createDatabase('mydb', { local: '.test' });
    expect(database).toBeInstanceOf(Database);
    expect(database.name).toBe('mydb');
  });
});
