/**
 * Shared test helpers for MongoLake client tests
 */

import { Database } from '../../../src/client/database.js';
import { Collection } from '../../../src/client/collection.js';
import { MemoryStorage } from '../../../src/storage/index.js';
import { validateDatabaseName } from '../../../src/validation/index.js';

// MongoLake has problematic imports for testing, so we'll use a minimal mock
class MockMongoLake {
  storage: MemoryStorage;
  private defaultDatabase: string = 'default';
  private databases: Map<string, Database> = new Map();

  constructor(_options: { local: string }) {
    this.storage = new MemoryStorage();
  }

  db(name?: string): Database {
    const dbName = name || this.defaultDatabase;

    // Validate database name to prevent path traversal attacks
    validateDatabaseName(dbName);

    if (!this.databases.has(dbName)) {
      this.databases.set(dbName, new Database(dbName, this.storage, { database: dbName }));
    }

    return this.databases.get(dbName)!;
  }

  async dropDatabase(name: string): Promise<void> {
    // Validate database name to prevent path traversal attacks
    validateDatabaseName(name);

    const files = await this.storage.list(`${name}/`);
    for (const file of files) {
      await this.storage.delete(file);
    }
    this.databases.delete(name);
  }

  async listDatabases(): Promise<string[]> {
    const files = await this.storage.list('');
    const databases = new Set<string>();

    for (const file of files) {
      const parts = file.split('/');
      if (parts.length > 0 && parts[0]) {
        const dbName = parts[0];
        try {
          validateDatabaseName(dbName);
          databases.add(dbName);
        } catch {
          // Skip invalid database names (system files, etc.)
        }
      }
    }

    return Array.from(databases);
  }
}

export { MockMongoLake as MongoLake };

/**
 * Create a MongoLake client with MemoryStorage for testing
 */
export function createTestClient(): MockMongoLake {
  return new MockMongoLake({ local: '.test-mongolake' });
}

/**
 * Create a Database with mocked storage
 */
export function createTestDatabase(name = 'testdb'): { db: Database; storage: MemoryStorage } {
  const storage = new MemoryStorage();
  const config = { database: name };
  const database = new Database(name, storage, config);
  return { db: database, storage };
}

/**
 * Create a Collection with mocked storage
 */
export function createTestCollection<T extends { _id?: string; [key: string]: unknown }>(
  collectionName = 'users',
  dbName = 'testdb'
): { collection: Collection<T>; storage: MemoryStorage; db: Database } {
  const storage = new MemoryStorage();
  const config = { database: dbName };
  const database = new Database(dbName, storage, config);
  const collection = database.collection<T>(collectionName);
  return { collection, storage, db: database };
}
