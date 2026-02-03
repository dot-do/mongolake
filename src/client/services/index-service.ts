/**
 * Index Service
 *
 * Encapsulates index management operations for MongoDB collections.
 * Delegates to IndexManager for actual index operations.
 */

import type { Document, IndexSpec, IndexOptions } from '@types';
import type { IndexManager } from '@mongolake/index/index-manager.js';

/**
 * Service for managing collection indexes.
 * Provides a clean interface for index CRUD operations.
 */
export class IndexService {
  constructor(private readonly indexManager: IndexManager) {}

  /**
   * Create an index on the collection.
   *
   * @param spec - Index specification (e.g., { age: 1 } for ascending index on age)
   * @param options - Index options (name, unique, sparse)
   * @param buildIndexCallback - Callback to get documents for building the index
   * @returns The index name
   *
   * @example
   * ```typescript
   * // Create ascending index on 'age' field
   * await indexService.createIndex({ age: 1 });
   *
   * // Create unique index with custom name
   * await indexService.createIndex({ email: 1 }, { unique: true, name: 'email_unique' });
   *
   * // Create text index for full-text search
   * await indexService.createIndex({ title: 'text', body: 'text' }, {
   *   name: 'content_text',
   *   weights: { title: 10, body: 1 }
   * });
   * ```
   */
  async createIndex(
    spec: IndexSpec,
    options?: IndexOptions,
    buildIndexCallback?: () => Promise<Document[]>
  ): Promise<string> {
    // Check if this is a text index
    const fields = Object.entries(spec);
    const textFields = fields.filter(([, value]) => value === 'text');

    if (textFields.length > 0) {
      // Create text index
      const fieldNames = textFields.map(([field]) => field);
      const indexName = await this.indexManager.createTextIndex(fieldNames, options);

      // Build the text index from existing documents if callback provided
      if (buildIndexCallback) {
        const docs = await buildIndexCallback();
        for (const doc of docs) {
          await this.indexManager.indexDocumentText(doc);
        }
      }

      // Persist index changes
      await this.indexManager.flush();

      return indexName;
    }

    // Create B-tree index
    const indexName = await this.indexManager.createIndex(spec, options);

    // Build the index from existing documents if callback provided
    if (buildIndexCallback) {
      const docs = await buildIndexCallback();
      for (const doc of docs) {
        await this.indexManager.indexDocument(doc);
      }
    }

    // Persist index changes
    await this.indexManager.flush();

    return indexName;
  }

  /**
   * Create multiple indexes.
   *
   * @param specs - Array of index specifications with options
   * @param buildIndexCallback - Callback to get documents for building indexes
   * @returns Array of index names
   */
  async createIndexes(
    specs: Array<{ key: IndexSpec; options?: IndexOptions }>,
    buildIndexCallback?: () => Promise<Document[]>
  ): Promise<string[]> {
    return Promise.all(specs.map((s) => this.createIndex(s.key, s.options, buildIndexCallback)));
  }

  /**
   * Drop an index by name.
   *
   * @param name - Index name to drop
   */
  async dropIndex(name: string): Promise<void> {
    // Try dropping as B-tree index first
    const dropped = await this.indexManager.dropIndex(name);
    if (!dropped) {
      // Try dropping as text index
      await this.indexManager.dropTextIndex(name);
    }
  }

  /**
   * List all indexes on the collection.
   *
   * @returns Array of index information with name and key specification
   */
  async listIndexes(): Promise<Array<{ name: string; key: IndexSpec }>> {
    // Get B-tree indexes
    const btreeIndexes = await this.indexManager.listIndexes();
    const result: Array<{ name: string; key: IndexSpec }> = btreeIndexes.map((idx) => ({
      name: idx.name,
      key: { [idx.field]: 1 } as IndexSpec,
    }));

    // Get text indexes
    const textIndexes = await this.indexManager.listTextIndexes();
    for (const idx of textIndexes) {
      const key: IndexSpec = {};
      for (const field of idx.fields) {
        key[field] = 'text';
      }
      result.push({ name: idx.name, key });
    }

    return result;
  }

  /**
   * Get the underlying IndexManager for advanced operations.
   * @internal
   */
  getIndexManager(): IndexManager {
    return this.indexManager;
  }
}
