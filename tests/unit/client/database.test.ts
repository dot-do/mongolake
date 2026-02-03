/**
 * Database Tests
 *
 * Tests for the Database class including:
 * - Collection access
 * - Collection listing and dropping
 * - Collection creation
 */

import { describe, it, expect } from 'vitest';
import { Collection, ValidationError } from '../../../src/client/index.js';
import { createTestDatabase } from './test-helpers.js';

describe('Database', () => {
  describe('collection()', () => {
    it('should return Collection instance', () => {
      const { db } = createTestDatabase();
      const collection = db.collection('users');
      expect(collection).toBeInstanceOf(Collection);
      expect(collection.name).toBe('users');
    });

    it('should cache and reuse Collection instances', () => {
      const { db } = createTestDatabase();
      const col1 = db.collection('users');
      const col2 = db.collection('users');
      expect(col1).toBe(col2);
    });

    it('should create separate instances for different collections', () => {
      const { db } = createTestDatabase();
      const users = db.collection('users');
      const posts = db.collection('posts');
      expect(users).not.toBe(posts);
    });

    it('should support generic type parameter', () => {
      interface User {
        _id?: string;
        name: string;
        email: string;
      }
      const { db } = createTestDatabase();
      const users = db.collection<User>('users');
      expect(users).toBeInstanceOf(Collection);
    });

    it('should reject invalid collection names with path traversal', () => {
      const { db } = createTestDatabase();
      expect(() => db.collection('../etc/passwd')).toThrow(ValidationError);
      expect(() => db.collection('..\\windows\\system32')).toThrow(ValidationError);
      expect(() => db.collection('users/../admin')).toThrow(ValidationError);
    });

    it('should reject collection names with dots', () => {
      const { db } = createTestDatabase();
      expect(() => db.collection('..')).toThrow(ValidationError);
      expect(() => db.collection('my.collection')).toThrow(ValidationError);
      expect(() => db.collection('.hidden')).toThrow(ValidationError);
    });

    it('should reject empty collection names', () => {
      const { db } = createTestDatabase();
      expect(() => db.collection('')).toThrow(ValidationError);
    });

    it('should reject collection names starting with underscore or hyphen', () => {
      const { db } = createTestDatabase();
      expect(() => db.collection('_internal')).toThrow(ValidationError);
      expect(() => db.collection('-invalid')).toThrow(ValidationError);
    });

    it('should reject collection names with null bytes', () => {
      const { db } = createTestDatabase();
      expect(() => db.collection('users\0admin')).toThrow(ValidationError);
    });
  });

  describe('listCollections()', () => {
    it('should return empty array for new database', async () => {
      const { db } = createTestDatabase();
      const collections = await db.listCollections();
      expect(collections).toEqual([]);
    });

    it('should list collections after data is written', async () => {
      const { db, storage } = createTestDatabase('mydb');

      // Simulate collection files
      await storage.put('mydb/users.parquet', new Uint8Array([1, 2, 3]));
      await storage.put('mydb/posts.parquet', new Uint8Array([4, 5, 6]));

      const collections = await db.listCollections();
      expect(collections).toContain('users');
      expect(collections).toContain('posts');
    });

    it('should not list manifest files as collections', async () => {
      const { db, storage } = createTestDatabase('mydb');

      await storage.put('mydb/users.parquet', new Uint8Array([1, 2, 3]));
      await storage.put('mydb/_manifest.json', new Uint8Array([1, 2, 3]));

      const collections = await db.listCollections();
      expect(collections).toContain('users');
      expect(collections).not.toContain('_manifest');
    });
  });

  describe('dropCollection()', () => {
    it('should remove collection files', async () => {
      const { db, storage } = createTestDatabase('mydb');

      await storage.put('mydb/users.parquet', new Uint8Array([1, 2, 3]));
      await storage.put('mydb/users_123.parquet', new Uint8Array([4, 5, 6]));
      await storage.put('mydb/posts.parquet', new Uint8Array([7, 8, 9]));

      const result = await db.dropCollection('users');
      expect(result).toBe(true);

      const files = await storage.list('mydb/');
      expect(files).not.toContain('mydb/users.parquet');
      expect(files).not.toContain('mydb/users_123.parquet');
      expect(files).toContain('mydb/posts.parquet');
    });

    it('should return false when collection does not exist', async () => {
      const { db } = createTestDatabase();
      const result = await db.dropCollection('nonexistent');
      expect(result).toBe(false);
    });

    it('should remove cached collection instance', async () => {
      const { db, storage } = createTestDatabase('mydb');

      await storage.put('mydb/users.parquet', new Uint8Array([1, 2, 3]));

      const col1 = db.collection('users');
      await db.dropCollection('users');
      const col2 = db.collection('users');

      expect(col1).not.toBe(col2);
    });

    it('should reject invalid collection names with path traversal', async () => {
      const { db } = createTestDatabase();
      await expect(db.dropCollection('../secret')).rejects.toThrow(ValidationError);
      await expect(db.dropCollection('..\\secret')).rejects.toThrow(ValidationError);
      await expect(db.dropCollection('..')).rejects.toThrow(ValidationError);
    });
  });

  describe('createCollection()', () => {
    it('should create and return collection', async () => {
      const { db } = createTestDatabase();
      const collection = await db.createCollection('users');
      expect(collection).toBeInstanceOf(Collection);
      expect(collection.name).toBe('users');
    });

    it('should support schema option', async () => {
      const { db } = createTestDatabase();
      const collection = await db.createCollection('users', {
        schema: { columns: { name: 'string' } },
      });
      expect(collection).toBeInstanceOf(Collection);
    });
  });

  describe('getPath()', () => {
    it('should return database name', () => {
      const { db } = createTestDatabase('myapp');
      expect(db.getPath()).toBe('myapp');
    });
  });
});
