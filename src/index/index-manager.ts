/**
 * Index Manager
 *
 * Manages B-tree indexes for MongoLake collections, handling:
 * - Index creation and deletion
 * - Index persistence to storage
 * - Index loading and caching
 * - Query optimization using indexes
 */

import { BTree, type SerializedBTree, type IndexMetadata } from './btree.js';
import { TextIndex, type SerializedTextIndex, type TextSearchResult } from './text-index.js';
import { GeoIndex, type SerializedGeoIndex, type GeoIndexType, type GeoDistanceResult } from './geo-index.js';
import {
  CompoundIndex,
  parseIndexSpec,
  generateCompoundIndexName,
  type SerializedCompoundIndex,
  type CompoundIndexMetadata,
} from './compound.js';
import type { StorageBackend } from '../storage/index.js';
import type { Document, Filter, IndexSpec, IndexOptions } from '../types.js';
import { getNestedValue } from '../utils/nested.js';
import { LRUCache } from '../utils/lru-cache.js';
import { logger } from '../utils/logger.js';

// ============================================================================
// Types
// ============================================================================

/** Cached index entry */
interface CachedIndex<K = unknown> {
  btree: BTree<K>;
  metadata: IndexMetadata;
  dirty: boolean;
}

/** Cached text index entry */
interface CachedTextIndex {
  textIndex: TextIndex;
  dirty: boolean;
}

/** Cached compound index entry */
interface CachedCompoundIndex {
  compoundIndex: CompoundIndex;
  metadata: CompoundIndexMetadata;
  dirty: boolean;
}

/** Cached geo index entry */
interface CachedGeoIndex {
  geoIndex: GeoIndex;
  dirty: boolean;
}

/** Geo index metadata */
export interface GeoIndexMetadata {
  name: string;
  field: string;
  type: GeoIndexType;
  min?: number;
  max?: number;
  bits?: number;
  createdAt: string;
}

/** Index scan result for query optimization */
export interface IndexScanResult {
  /** Document IDs that match the index condition */
  docIds: string[];
  /** Whether this is an exact match (vs. partial) */
  exact: boolean;
  /** The index used */
  indexName: string;
}

/** Query plan information */
export interface QueryPlan {
  /** Whether an index can be used */
  useIndex: boolean;
  /** Name of the index to use (if any) */
  indexName?: string;
  /** Field being queried */
  field?: string;
  /** Type of query operation */
  operation?: 'eq' | 'range' | 'in';
  /** Whether full collection scan is needed */
  fullScan: boolean;
}

// ============================================================================
// Index Manager
// ============================================================================

/**
 * Manages indexes for a collection
 */
export class IndexManager {
  /** Collection name */
  private readonly collectionName: string;

  /** Database path */
  private readonly dbPath: string;

  /** Storage backend */
  private readonly storage: StorageBackend;

  /** Cached indexes with LRU eviction (max 100 indexes per collection) */
  private indexes: LRUCache<string, CachedIndex> = new LRUCache({
    maxSize: 100,
    onEvict: (_key, value) => {
      // Mark evicted indexes as dirty so they're saved if accessed again
      if (value.dirty) {
        // In a production system, we might want to persist before eviction
        logger.warn('Evicting dirty index', {
          indexName: _key,
          collection: this.collectionName,
        });
      }
    },
  });

  /** Whether indexes have been loaded */
  private loaded: boolean = false;

  /** Text indexes (separate from B-tree indexes) */
  private textIndexes: Map<string, CachedTextIndex> = new Map();

  /** Compound indexes */
  private compoundIndexes: Map<string, CachedCompoundIndex> = new Map();

  /** Geo indexes (2d and 2dsphere) */
  private geoIndexes: Map<string, CachedGeoIndex> = new Map();

  constructor(dbPath: string, collectionName: string, storage: StorageBackend) {
    this.dbPath = dbPath;
    this.collectionName = collectionName;
    this.storage = storage;
  }

  // --------------------------------------------------------------------------
  // Index Management
  // --------------------------------------------------------------------------

  /**
   * Ensure the _id index exists
   * This is called automatically when a collection is created/loaded
   */
  async ensureIdIndex(): Promise<void> {
    await this.ensureLoaded();

    // Check if _id index already exists
    if (this.indexes.has('_id_')) {
      return;
    }

    // Create the _id index with unique constraint
    const btree = new BTree(
      '_id_',
      '_id',
      64, // minDegree for good performance
      undefined,
      true // unique index
    );

    const metadata: IndexMetadata = {
      name: '_id_',
      field: '_id',
      unique: true,
      sparse: false,
      createdAt: new Date().toISOString(),
    };

    // Cache the index
    this.indexes.set('_id_', {
      btree,
      metadata,
      dirty: true,
    });

    // Persist immediately
    await this.saveIndex('_id_');
  }

  /**
   * Create a new index
   *
   * @param spec - Index specification (field: direction)
   * @param options - Index options
   * @returns The index name
   */
  async createIndex(spec: IndexSpec, options?: IndexOptions): Promise<string> {
    await this.ensureLoaded();

    // Get the fields to index
    const fields = Object.keys(spec);

    // Handle compound indexes (multiple fields)
    if (fields.length > 1) {
      return this.createCompoundIndex(spec, options);
    }

    const field = fields[0]!;
    const indexType = spec[field];

    // Handle geo indexes (2d or 2dsphere)
    if (indexType === '2d' || indexType === '2dsphere') {
      return this.createGeoIndex(field, indexType, options);
    }

    // Handle text indexes
    if (indexType === 'text') {
      return this.createTextIndex([field], options);
    }

    const name = options?.name || `${field}_${indexType}`;

    // Check if index already exists
    if (this.indexes.has(name)) {
      return name;
    }

    // Create new B-tree index
    const btree = new BTree(
      name,
      field!,
      64, // minDegree for good performance
      undefined,
      options?.unique || false
    );

    const metadata: IndexMetadata = {
      name,
      field: field!,
      unique: options?.unique || false,
      sparse: options?.sparse || false,
      createdAt: new Date().toISOString(),
    };

    // Cache the index
    this.indexes.set(name, {
      btree,
      metadata,
      dirty: true,
    });

    // Persist immediately
    await this.saveIndex(name);

    return name;
  }

  /**
   * Drop an index
   *
   * @param name - Index name to drop
   * @returns true if index was dropped
   */
  async dropIndex(name: string): Promise<boolean> {
    await this.ensureLoaded();

    // Cannot drop _id index
    if (name === '_id_') {
      throw new Error('Cannot drop _id index');
    }

    if (!this.indexes.has(name)) {
      return false;
    }

    // Remove from cache
    this.indexes.delete(name);

    // Delete from storage
    const indexPath = this.getIndexPath(name);
    try {
      await this.storage.delete(indexPath);
    } catch {
      // Ignore errors if file doesn't exist
    }

    // Update index list
    await this.saveIndexList();

    return true;
  }

  /**
   * List all indexes
   *
   * @returns Array of index metadata (includes both single-field and compound indexes)
   */
  async listIndexes(): Promise<(IndexMetadata & { fields?: string[]; directions?: (1 | -1)[] })[]> {
    await this.ensureLoaded();

    const singleFieldIndexes = this.indexes.values().map((cached) => cached.metadata);

    // Also include compound indexes in the list
    const compoundIndexes = Array.from(this.compoundIndexes.values()).map((cached) => ({
      name: cached.metadata.name,
      field: cached.metadata.fields.map((f) => f.field).join(', '),
      unique: cached.metadata.unique,
      sparse: cached.metadata.sparse,
      createdAt: cached.metadata.createdAt,
      fields: cached.metadata.fields.map((f) => f.field),
      directions: cached.metadata.fields.map((f) => f.direction),
    }));

    return [...singleFieldIndexes, ...compoundIndexes];
  }

  /**
   * Get an index by name
   *
   * @param name - Index name
   * @returns The B-tree index or undefined (returns compound index btree if found)
   */
  async getIndex(name: string): Promise<BTree | undefined> {
    await this.ensureLoaded();

    // First check single-field indexes
    const singleField = this.indexes.get(name);
    if (singleField) {
      return singleField.btree;
    }

    // For compound indexes, we need a different approach - they use CompoundIndex
    // This method is primarily for single-field B-tree indexes
    return undefined;
  }

  /**
   * Get an index by field name
   *
   * @param field - Field name
   * @returns The B-tree index or undefined
   */
  async getIndexByField(field: string): Promise<BTree | undefined> {
    await this.ensureLoaded();

    const cached = this.indexes.values().find((c) => c.metadata.field === field);
    return cached?.btree;
  }

  // --------------------------------------------------------------------------
  // Index Operations
  // --------------------------------------------------------------------------

  /**
   * Index a document
   *
   * @param doc - Document to index
   */
  async indexDocument(doc: Document): Promise<void> {
    await this.ensureLoaded();

    const docId = this.extractDocId(doc);

    // Index in single-field B-tree indexes
    for (const cached of this.indexes.values()) {
      const value = getNestedValue(doc, cached.metadata.field);

      // Skip undefined/null values for sparse indexes
      if (cached.metadata.sparse && (value === undefined || value === null)) {
        continue;
      }

      // Handle array values - index each element
      if (Array.isArray(value)) {
        for (const item of value) {
          cached.btree.insert(item, docId);
        }
      } else {
        cached.btree.insert(value, docId);
      }

      cached.dirty = true;
    }

    // Index in compound indexes
    await this.indexDocumentCompound(doc);
  }

  /**
   * Remove a document from all indexes
   *
   * @param doc - Document to remove
   */
  async unindexDocument(doc: Document): Promise<void> {
    await this.ensureLoaded();

    const docId = this.extractDocId(doc);

    // Remove from single-field B-tree indexes
    for (const cached of this.indexes.values()) {
      const value = getNestedValue(doc, cached.metadata.field);

      if (value === undefined || value === null) {
        continue;
      }

      // Handle array values
      if (Array.isArray(value)) {
        for (const item of value) {
          cached.btree.delete(item, docId);
        }
      } else {
        cached.btree.delete(value, docId);
      }

      cached.dirty = true;
    }

    // Remove from compound indexes
    await this.unindexDocumentCompound(doc);
  }

  /**
   * Update indexes for a document that has been modified.
   * This efficiently handles non-_id index updates by:
   * 1. Unindexing old values from all non-_id indexes
   * 2. Indexing new values to all non-_id indexes
   *
   * The _id index is skipped since _id never changes on update.
   *
   * @param oldDoc - The document before the update
   * @param newDoc - The document after the update
   */
  async updateDocumentIndexes(oldDoc: Document, newDoc: Document): Promise<void> {
    await this.ensureLoaded();

    const docId = this.extractDocId(newDoc);

    // Update single-field B-tree indexes (skip _id index)
    for (const cached of this.indexes.values()) {
      // Skip the _id index - _id never changes on update
      if (cached.metadata.field === '_id') {
        continue;
      }

      const oldValue = getNestedValue(oldDoc, cached.metadata.field);
      const newValue = getNestedValue(newDoc, cached.metadata.field);

      // Skip if values are identical (no index update needed)
      if (this.valuesEqual(oldValue, newValue)) {
        continue;
      }

      // Unindex old value(s)
      if (oldValue !== undefined && oldValue !== null) {
        if (Array.isArray(oldValue)) {
          for (const item of oldValue) {
            cached.btree.delete(item, docId);
          }
        } else {
          cached.btree.delete(oldValue, docId);
        }
      }

      // Index new value(s)
      // Skip undefined/null values for sparse indexes
      if (cached.metadata.sparse && (newValue === undefined || newValue === null)) {
        cached.dirty = true;
        continue;
      }

      if (newValue !== undefined && newValue !== null) {
        if (Array.isArray(newValue)) {
          for (const item of newValue) {
            cached.btree.insert(item, docId);
          }
        } else {
          cached.btree.insert(newValue, docId);
        }
      }

      cached.dirty = true;
    }

    // Update compound indexes
    await this.updateDocumentCompoundIndexes(oldDoc, newDoc);

    // Update text indexes
    await this.updateDocumentTextIndexes(oldDoc, newDoc);

    // Update geo indexes
    await this.updateDocumentGeoIndexes(oldDoc, newDoc);
  }

  /**
   * Update compound indexes for a modified document
   */
  private async updateDocumentCompoundIndexes(oldDoc: Document, newDoc: Document): Promise<void> {
    const docId = this.extractDocId(newDoc);

    for (const cached of this.compoundIndexes.values()) {
      // Check if any indexed field changed
      const fields = cached.metadata.fields.map(f => f.field);
      let hasChange = false;

      for (const field of fields) {
        const oldValue = getNestedValue(oldDoc, field);
        const newValue = getNestedValue(newDoc, field);
        if (!this.valuesEqual(oldValue, newValue)) {
          hasChange = true;
          break;
        }
      }

      if (!hasChange) {
        continue;
      }

      // Unindex old compound entry
      cached.compoundIndex.unindexDocument(docId, oldDoc);

      // Index new compound entry
      try {
        cached.compoundIndex.indexDocument(docId, newDoc);
      } catch (error) {
        // Re-throw unique constraint violations
        if (error instanceof Error && error.message.includes('Duplicate key')) {
          throw error;
        }
        logger.warn('Failed to update compound index', {
          indexName: cached.metadata.name,
          collection: this.collectionName,
          error,
        });
      }

      cached.dirty = true;
    }
  }

  /**
   * Update text indexes for a modified document
   */
  private async updateDocumentTextIndexes(oldDoc: Document, newDoc: Document): Promise<void> {
    const docId = this.extractDocId(newDoc);

    for (const cached of this.textIndexes.values()) {
      // Check if any indexed field changed
      const fields = cached.textIndex.fields;
      let hasChange = false;

      for (const field of fields) {
        const oldValue = getNestedValue(oldDoc, field);
        const newValue = getNestedValue(newDoc, field);
        if (!this.valuesEqual(oldValue, newValue)) {
          hasChange = true;
          break;
        }
      }

      if (!hasChange) {
        continue;
      }

      // Unindex old document
      cached.textIndex.unindexDocument(docId);

      // Index new document
      cached.textIndex.indexDocument(docId, newDoc as Record<string, unknown>);

      cached.dirty = true;
    }
  }

  /**
   * Update geo indexes for a modified document
   */
  private async updateDocumentGeoIndexes(oldDoc: Document, newDoc: Document): Promise<void> {
    const docId = this.extractDocId(newDoc);

    for (const cached of this.geoIndexes.values()) {
      const field = cached.geoIndex.field;
      const oldValue = getNestedValue(oldDoc, field);
      const newValue = getNestedValue(newDoc, field);

      // Skip if geo field hasn't changed
      if (this.valuesEqual(oldValue, newValue)) {
        continue;
      }

      // Unindex old location
      cached.geoIndex.unindexDocument(docId);

      // Index new location
      try {
        cached.geoIndex.indexDocument(docId, newDoc as Record<string, unknown>);
      } catch (error) {
        logger.warn('Failed to update geo index', {
          indexName: cached.geoIndex.name,
          collection: this.collectionName,
          docId,
          error,
        });
      }

      cached.dirty = true;
    }
  }

  /**
   * Check if two values are equal (handles arrays and objects)
   */
  private valuesEqual(a: unknown, b: unknown): boolean {
    if (a === b) return true;
    if (a === null || b === null) return a === b;
    if (a === undefined || b === undefined) return a === b;

    if (Array.isArray(a) && Array.isArray(b)) {
      if (a.length !== b.length) return false;
      for (let i = 0; i < a.length; i++) {
        if (!this.valuesEqual(a[i], b[i])) return false;
      }
      return true;
    }

    if (typeof a === 'object' && typeof b === 'object') {
      const keysA = Object.keys(a as object);
      const keysB = Object.keys(b as object);
      if (keysA.length !== keysB.length) return false;
      for (const key of keysA) {
        if (!this.valuesEqual((a as Record<string, unknown>)[key], (b as Record<string, unknown>)[key])) {
          return false;
        }
      }
      return true;
    }

    return false;
  }

  /**
   * Rebuild all indexes from documents
   *
   * @param documents - All documents in the collection
   */
  async rebuildIndexes(documents: Document[]): Promise<void> {
    await this.ensureLoaded();

    // Clear all single-field indexes
    this.indexes.forEach((cached) => {
      cached.btree.clear();
      cached.dirty = true;
    });

    // Clear all compound indexes
    this.compoundIndexes.forEach((cached) => {
      cached.compoundIndex.clear();
      cached.dirty = true;
    });

    // Re-index all documents
    for (const doc of documents) {
      await this.indexDocument(doc);
    }

    // Persist all indexes
    await this.flush();
  }

  // --------------------------------------------------------------------------
  // Query Optimization
  // --------------------------------------------------------------------------

  /**
   * Analyze a filter to determine the best query plan
   *
   * @param filter - Query filter
   * @returns Query plan information
   */
  async analyzeQuery(filter: Filter): Promise<QueryPlan> {
    await this.ensureLoaded();

    if (!filter || Object.keys(filter).length === 0) {
      return { useIndex: false, fullScan: true };
    }

    // First, check for compound index matches (they're often more selective)
    const compoundMatch = await this.findBestCompoundIndex(filter as Record<string, unknown>);
    if (compoundMatch && compoundMatch.equalityFields.length >= 1) {
      // Calculate the operation type based on what the compound index covers
      let operation: 'eq' | 'range' | 'in' = 'eq';
      if (compoundMatch.rangeField) {
        operation = 'range';
      }

      return {
        useIndex: true,
        indexName: compoundMatch.index.name,
        field: compoundMatch.coveredFields.join(', '),
        operation,
        fullScan: false,
      };
    }

    // Check each filter field for matching single-field index
    for (const [field, condition] of Object.entries(filter)) {
      // Skip logical operators
      if (field.startsWith('$')) continue;

      // Check if we have an index for this field
      const index = await this.getIndexByField(field);
      if (!index) continue;

      // Determine operation type
      let operation: 'eq' | 'range' | 'in' = 'eq';

      if (typeof condition === 'object' && condition !== null) {
        if ('$in' in condition) {
          operation = 'in';
        } else if ('$gt' in condition || '$gte' in condition ||
                   '$lt' in condition || '$lte' in condition) {
          operation = 'range';
        } else if ('$eq' in condition) {
          operation = 'eq';
        } else {
          // Complex condition - may need full scan
          continue;
        }
      }

      return {
        useIndex: true,
        indexName: index.name,
        field,
        operation,
        fullScan: false,
      };
    }

    return { useIndex: false, fullScan: true };
  }

  /**
   * Execute an index scan based on a filter condition
   *
   * @param indexName - Index to use
   * @param filter - Query filter for the indexed field
   * @returns Matching document IDs
   */
  async scanIndex(indexName: string, field: string, filter: Filter): Promise<IndexScanResult> {
    await this.ensureLoaded();

    // Check if this is a compound index
    if (this.compoundIndexes.has(indexName)) {
      return this.scanCompoundIndex(indexName, filter as Record<string, unknown>);
    }

    const index = this.indexes.get(indexName)?.btree;
    if (!index) {
      throw new Error(`Index not found: ${indexName}`);
    }

    const condition = filter[field];

    // Simple equality
    if (typeof condition !== 'object' || condition === null) {
      const docIds = index.search(condition);
      return { docIds, exact: true, indexName };
    }

    // $eq operator
    if ('$eq' in condition) {
      const docIds = index.search(condition.$eq);
      return { docIds, exact: true, indexName };
    }

    // $in operator
    if ('$in' in condition && Array.isArray(condition.$in)) {
      const docIds = new Set<string>();
      for (const value of condition.$in) {
        for (const id of index.search(value)) {
          docIds.add(id);
        }
      }
      return { docIds: Array.from(docIds), exact: true, indexName };
    }

    // Range operators
    let minKey: unknown = null;
    let maxKey: unknown = null;
    let minInclusive = true;
    let maxInclusive = true;

    if ('$gt' in condition) {
      minKey = condition.$gt;
      minInclusive = false;
    }
    if ('$gte' in condition) {
      minKey = condition.$gte;
      minInclusive = true;
    }
    if ('$lt' in condition) {
      maxKey = condition.$lt;
      maxInclusive = false;
    }
    if ('$lte' in condition) {
      maxKey = condition.$lte;
      maxInclusive = true;
    }

    // For now, use inclusive range and filter in memory
    // A proper implementation would handle exclusive bounds in the B-tree
    const entries = index.range(minKey, maxKey);
    const docIds: string[] = [];

    for (const [key, ids] of entries) {
      // Filter based on inclusive/exclusive bounds
      if (!minInclusive && key === minKey) continue;
      if (!maxInclusive && key === maxKey) continue;

      docIds.push(...ids);
    }

    return { docIds, exact: true, indexName };
  }

  // --------------------------------------------------------------------------
  // Persistence
  // --------------------------------------------------------------------------

  /**
   * Save all dirty indexes to storage
   */
  async flush(): Promise<void> {
    // Save B-tree indexes
    for (const [name, cached] of this.indexes.entries()) {
      if (cached.dirty) {
        await this.saveIndex(name);
        cached.dirty = false;
      }
    }

    // Save text indexes
    for (const [name, cached] of this.textIndexes.entries()) {
      if (cached.dirty) {
        await this.saveTextIndex(name);
        cached.dirty = false;
      }
    }

    // Save compound indexes
    for (const [name, cached] of this.compoundIndexes.entries()) {
      if (cached.dirty) {
        await this.saveCompoundIndex(name);
        cached.dirty = false;
      }
    }

    // Save geo indexes
    for (const [name, cached] of this.geoIndexes.entries()) {
      if (cached.dirty) {
        await this.saveGeoIndex(name);
        cached.dirty = false;
      }
    }
  }

  /**
   * Save a specific index to storage
   */
  private async saveIndex(name: string): Promise<void> {
    const cached = this.indexes.get(name);
    if (!cached) return;

    const serialized = cached.btree.serialize();
    const data = JSON.stringify({
      metadata: cached.metadata,
      btree: serialized,
    });

    await this.storage.put(
      this.getIndexPath(name),
      new TextEncoder().encode(data)
    );

    // Update index list
    await this.saveIndexList();
  }

  /**
   * Load all indexes from storage
   */
  private async loadIndexes(): Promise<void> {
    // Load B-tree index list
    const listPath = this.getIndexListPath();
    const listData = await this.storage.get(listPath);

    if (listData) {
      try {
        const indexNames = JSON.parse(new TextDecoder().decode(listData)) as string[];

        // Load each index
        for (const name of indexNames) {
          await this.loadIndex(name);
        }
      } catch {
        // Index list corrupted, start fresh
        logger.warn('Index list corrupted, starting fresh', {
          collection: this.collectionName,
          indexType: 'btree',
        });
      }
    }

    // Also load text indexes
    await this.loadTextIndexes();

    // Also load compound indexes
    await this.loadCompoundIndexes();

    // Also load geo indexes
    await this.loadGeoIndexes();

    this.loaded = true;
  }

  /**
   * Load a specific index from storage
   */
  private async loadIndex(name: string): Promise<void> {
    const data = await this.storage.get(this.getIndexPath(name));
    if (!data) return;

    try {
      const parsed = JSON.parse(new TextDecoder().decode(data)) as {
        metadata: IndexMetadata;
        btree: SerializedBTree<unknown>;
      };

      const btree = BTree.deserialize(parsed.btree);

      this.indexes.set(name, {
        btree,
        metadata: parsed.metadata,
        dirty: false,
      });
    } catch (error) {
      logger.warn('Failed to load index', {
        indexName: name,
        collection: this.collectionName,
        indexType: 'btree',
        error,
      });
    }
  }

  /**
   * Save the list of index names
   */
  private async saveIndexList(): Promise<void> {
    const names = Array.from(this.indexes.keys());
    const data = JSON.stringify(names);

    await this.storage.put(
      this.getIndexListPath(),
      new TextEncoder().encode(data)
    );
  }

  /**
   * Ensure indexes are loaded from storage
   */
  private async ensureLoaded(): Promise<void> {
    if (!this.loaded) {
      await this.loadIndexes();
    }
  }

  // --------------------------------------------------------------------------
  // Text Index Operations
  // --------------------------------------------------------------------------

  /**
   * Create a text index for full-text search
   *
   * @param fields - Fields to index (must have 'text' value in spec)
   * @param options - Index options including weights and default_language
   * @returns The index name
   *
   * @example
   * ```typescript
   * // Create text index on title and body fields
   * await indexManager.createTextIndex(
   *   ['title', 'body'],
   *   { name: 'content_text', weights: { title: 10, body: 1 } }
   * );
   * ```
   */
  async createTextIndex(
    fields: string[],
    options?: IndexOptions
  ): Promise<string> {
    await this.ensureLoaded();

    if (fields.length === 0) {
      throw new Error('Text index must have at least one field');
    }

    const name = options?.name || `${fields.join('_')}_text`;

    // Check if text index already exists
    if (this.textIndexes.has(name)) {
      return name;
    }

    // Create new text index
    const textIndex = new TextIndex(
      name,
      fields,
      options?.weights || {},
      options?.default_language || 'english'
    );

    // Cache the index
    this.textIndexes.set(name, {
      textIndex,
      dirty: true,
    });

    // Persist immediately
    await this.saveTextIndex(name);

    return name;
  }

  /**
   * Drop a text index
   *
   * @param name - Index name to drop
   * @returns true if index was dropped
   */
  async dropTextIndex(name: string): Promise<boolean> {
    await this.ensureLoaded();

    if (!this.textIndexes.has(name)) {
      return false;
    }

    // Remove from cache
    this.textIndexes.delete(name);

    // Delete from storage
    const indexPath = this.getTextIndexPath(name);
    try {
      await this.storage.delete(indexPath);
    } catch {
      // Ignore errors if file doesn't exist
    }

    // Update text index list
    await this.saveTextIndexList();

    return true;
  }

  /**
   * Get a text index by name
   *
   * @param name - Index name
   * @returns The text index or undefined
   */
  async getTextIndex(name: string): Promise<TextIndex | undefined> {
    await this.ensureLoaded();
    return this.textIndexes.get(name)?.textIndex;
  }

  /**
   * Get the first available text index
   *
   * @returns The text index or undefined
   */
  async getFirstTextIndex(): Promise<TextIndex | undefined> {
    await this.ensureLoaded();
    const first = this.textIndexes.values().next();
    return first.done ? undefined : first.value.textIndex;
  }

  /**
   * Check if collection has a text index
   */
  async hasTextIndex(): Promise<boolean> {
    await this.ensureLoaded();
    return this.textIndexes.size > 0;
  }

  /**
   * List all text indexes
   *
   * @returns Array of text index names and fields
   */
  async listTextIndexes(): Promise<Array<{ name: string; fields: string[] }>> {
    await this.ensureLoaded();
    return Array.from(this.textIndexes.values()).map((cached) => ({
      name: cached.textIndex.name,
      fields: cached.textIndex.fields,
    }));
  }

  /**
   * Index a document in all text indexes
   *
   * @param doc - Document to index
   */
  async indexDocumentText(doc: Document): Promise<void> {
    await this.ensureLoaded();

    const docId = this.extractDocId(doc);

    for (const cached of this.textIndexes.values()) {
      cached.textIndex.indexDocument(docId, doc as Record<string, unknown>);
      cached.dirty = true;
    }
  }

  /**
   * Remove a document from all text indexes
   *
   * @param docId - Document ID to remove
   */
  async unindexDocumentText(docId: string): Promise<void> {
    await this.ensureLoaded();

    for (const cached of this.textIndexes.values()) {
      cached.textIndex.unindexDocument(docId);
      cached.dirty = true;
    }
  }

  /**
   * Search text indexes
   *
   * @param query - Search query string
   * @returns Array of matching document IDs with scores
   */
  async textSearch(query: string): Promise<TextSearchResult[]> {
    await this.ensureLoaded();

    const textIndex = await this.getFirstTextIndex();
    if (!textIndex) {
      throw new Error('No text index available. Create a text index first.');
    }

    return textIndex.search(query);
  }

  /**
   * Get text search scores for documents
   *
   * @param query - Search query string
   * @returns Map of document ID to score
   */
  async getTextScores(query: string): Promise<Map<string, number>> {
    await this.ensureLoaded();

    const textIndex = await this.getFirstTextIndex();
    if (!textIndex) {
      return new Map();
    }

    return textIndex.getScores(query);
  }

  /**
   * Save a text index to storage
   */
  private async saveTextIndex(name: string): Promise<void> {
    const cached = this.textIndexes.get(name);
    if (!cached) return;

    const serialized = cached.textIndex.serialize();
    const data = JSON.stringify(serialized);

    await this.storage.put(
      this.getTextIndexPath(name),
      new TextEncoder().encode(data)
    );

    // Update text index list
    await this.saveTextIndexList();
  }

  /**
   * Load a text index from storage
   */
  private async loadTextIndex(name: string): Promise<void> {
    const data = await this.storage.get(this.getTextIndexPath(name));
    if (!data) return;

    try {
      const parsed = JSON.parse(new TextDecoder().decode(data)) as SerializedTextIndex;
      const textIndex = TextIndex.deserialize(parsed);

      this.textIndexes.set(name, {
        textIndex,
        dirty: false,
      });
    } catch (error) {
      logger.warn('Failed to load text index', {
        indexName: name,
        collection: this.collectionName,
        indexType: 'text',
        error,
      });
    }
  }

  /**
   * Save the list of text index names
   */
  private async saveTextIndexList(): Promise<void> {
    const names = Array.from(this.textIndexes.keys());
    const data = JSON.stringify(names);

    await this.storage.put(
      this.getTextIndexListPath(),
      new TextEncoder().encode(data)
    );
  }

  /**
   * Load all text indexes from storage
   */
  private async loadTextIndexes(): Promise<void> {
    const listPath = this.getTextIndexListPath();
    const listData = await this.storage.get(listPath);

    if (!listData) {
      return;
    }

    try {
      const indexNames = JSON.parse(new TextDecoder().decode(listData)) as string[];

      for (const name of indexNames) {
        await this.loadTextIndex(name);
      }
    } catch {
      logger.warn('Text index list corrupted, starting fresh', {
        collection: this.collectionName,
        indexType: 'text',
      });
    }
  }

  /**
   * Get the storage path for a text index
   */
  private getTextIndexPath(name: string): string {
    return `${this.dbPath}/${this.collectionName}/_indexes/text_${name}.json`;
  }

  /**
   * Get the storage path for the text index list
   */
  private getTextIndexListPath(): string {
    return `${this.dbPath}/${this.collectionName}/_indexes/_text_list.json`;
  }

  // --------------------------------------------------------------------------
  // Compound Index Operations
  // --------------------------------------------------------------------------

  /**
   * Create a compound index on multiple fields
   *
   * @param spec - Index specification with multiple fields
   * @param options - Index options
   * @returns The index name
   *
   * @example
   * ```typescript
   * // Create compound index on name (ascending) and age (descending)
   * await indexManager.createCompoundIndex(
   *   { name: 1, age: -1 },
   *   { unique: true }
   * );
   * ```
   */
  async createCompoundIndex(spec: IndexSpec, options?: IndexOptions): Promise<string> {
    await this.ensureLoaded();

    const fields = parseIndexSpec(spec);
    if (fields.length < 2) {
      throw new Error('Compound index must have at least two fields');
    }

    const name = options?.name || generateCompoundIndexName(spec);

    // Check if index already exists
    if (this.compoundIndexes.has(name)) {
      return name;
    }

    // Create new compound index
    const compoundIndex = new CompoundIndex(
      name,
      fields,
      options?.unique || false,
      options?.sparse || false
    );

    const metadata: CompoundIndexMetadata = {
      name,
      fields,
      unique: options?.unique || false,
      sparse: options?.sparse || false,
      createdAt: new Date().toISOString(),
    };

    // Cache the index
    this.compoundIndexes.set(name, {
      compoundIndex,
      metadata,
      dirty: true,
    });

    // Persist immediately
    await this.saveCompoundIndex(name);

    return name;
  }

  /**
   * Drop a compound index
   *
   * @param name - Index name to drop
   * @returns true if index was dropped
   */
  async dropCompoundIndex(name: string): Promise<boolean> {
    await this.ensureLoaded();

    if (!this.compoundIndexes.has(name)) {
      return false;
    }

    // Remove from cache
    this.compoundIndexes.delete(name);

    // Delete from storage
    const indexPath = this.getCompoundIndexPath(name);
    try {
      await this.storage.delete(indexPath);
    } catch {
      // Ignore errors if file doesn't exist
    }

    // Update compound index list
    await this.saveCompoundIndexList();

    return true;
  }

  /**
   * Get a compound index by name
   *
   * @param name - Index name
   * @returns The compound index or undefined
   */
  async getCompoundIndex(name: string): Promise<CompoundIndex | undefined> {
    await this.ensureLoaded();
    return this.compoundIndexes.get(name)?.compoundIndex;
  }

  /**
   * List all compound indexes
   *
   * @returns Array of compound index metadata
   */
  async listCompoundIndexes(): Promise<CompoundIndexMetadata[]> {
    await this.ensureLoaded();
    return Array.from(this.compoundIndexes.values()).map((cached) => cached.metadata);
  }

  /**
   * Find the best compound index for a filter
   *
   * @param filter - Query filter
   * @returns Best compound index and match info, or undefined
   */
  async findBestCompoundIndex(
    filter: Record<string, unknown>
  ): Promise<{ index: CompoundIndex; coveredFields: string[]; equalityFields: string[]; rangeField?: string } | undefined> {
    await this.ensureLoaded();

    let bestMatch: { index: CompoundIndex; coveredFields: string[]; equalityFields: string[]; rangeField?: string } | undefined;
    let bestScore = 0;

    for (const cached of this.compoundIndexes.values()) {
      const support = cached.compoundIndex.canSupportFilter(filter);

      if (!support.canUse) continue;

      // Score based on number of equality fields plus optional range
      const score = support.equalityFields.length * 2 + (support.rangeField ? 1 : 0);

      if (score > bestScore) {
        bestScore = score;
        bestMatch = {
          index: cached.compoundIndex,
          coveredFields: support.coveredFields,
          equalityFields: support.equalityFields,
          rangeField: support.rangeField,
        };
      }
    }

    return bestMatch;
  }

  /**
   * Index a document in all compound indexes
   *
   * @param doc - Document to index
   */
  async indexDocumentCompound(doc: Document): Promise<void> {
    await this.ensureLoaded();

    const docId = this.extractDocId(doc);

    for (const cached of this.compoundIndexes.values()) {
      try {
        cached.compoundIndex.indexDocument(docId, doc);
        cached.dirty = true;
      } catch (error) {
        // Re-throw unique constraint violations
        if (error instanceof Error && error.message.includes('Duplicate key')) {
          throw error;
        }
        // Log but don't fail for other errors (e.g., sparse index skip)
        logger.warn('Failed to index document in compound index', {
          indexName: cached.metadata.name,
          collection: this.collectionName,
          docId,
          error,
        });
      }
    }
  }

  /**
   * Remove a document from all compound indexes
   *
   * @param doc - Document to remove
   */
  async unindexDocumentCompound(doc: Document): Promise<void> {
    await this.ensureLoaded();

    const docId = this.extractDocId(doc);

    for (const cached of this.compoundIndexes.values()) {
      cached.compoundIndex.unindexDocument(docId, doc);
      cached.dirty = true;
    }
  }

  /**
   * Execute a scan on a compound index
   *
   * @param indexName - Compound index name
   * @param filter - Query filter
   * @returns Matching document IDs
   */
  async scanCompoundIndex(indexName: string, filter: Record<string, unknown>): Promise<IndexScanResult> {
    await this.ensureLoaded();

    const cached = this.compoundIndexes.get(indexName);
    if (!cached) {
      throw new Error(`Compound index not found: ${indexName}`);
    }

    const index = cached.compoundIndex;
    const support = index.canSupportFilter(filter);

    if (!support.canUse) {
      return { docIds: [], exact: false, indexName };
    }

    // Build conditions array for searchByPrefix
    const conditions: Array<{
      field: string;
      value?: unknown;
      op?: 'eq' | 'gt' | 'gte' | 'lt' | 'lte' | 'in';
      values?: unknown[];
    }> = [];

    for (const fieldName of support.coveredFields) {
      const condition = filter[fieldName];

      if (condition === undefined) continue;

      // Simple value = equality
      if (condition === null || typeof condition !== 'object') {
        conditions.push({ field: fieldName, value: condition, op: 'eq' });
        continue;
      }

      const cond = condition as Record<string, unknown>;

      // Handle operators
      if ('$eq' in cond) {
        conditions.push({ field: fieldName, value: cond.$eq, op: 'eq' });
      } else if ('$in' in cond && Array.isArray(cond.$in)) {
        conditions.push({ field: fieldName, values: cond.$in, op: 'in' });
      } else if ('$gt' in cond) {
        conditions.push({ field: fieldName, value: cond.$gt, op: 'gt' });
      } else if ('$gte' in cond) {
        conditions.push({ field: fieldName, value: cond.$gte, op: 'gte' });
      } else if ('$lt' in cond) {
        conditions.push({ field: fieldName, value: cond.$lt, op: 'lt' });
      } else if ('$lte' in cond) {
        conditions.push({ field: fieldName, value: cond.$lte, op: 'lte' });
      }
    }

    const docIds = index.searchByPrefix(conditions);

    // Determine if this is an exact match (all filter fields covered)
    const filterFields = Object.keys(filter).filter((k) => !k.startsWith('$'));
    const exact = filterFields.every((f) => support.coveredFields.includes(f));

    return { docIds, exact, indexName };
  }

  /**
   * Save a compound index to storage
   */
  private async saveCompoundIndex(name: string): Promise<void> {
    const cached = this.compoundIndexes.get(name);
    if (!cached) return;

    const serialized = cached.compoundIndex.serialize();
    const data = JSON.stringify(serialized);

    await this.storage.put(
      this.getCompoundIndexPath(name),
      new TextEncoder().encode(data)
    );

    // Update compound index list
    await this.saveCompoundIndexList();
  }

  /**
   * Load a compound index from storage
   */
  private async loadCompoundIndex(name: string): Promise<void> {
    const data = await this.storage.get(this.getCompoundIndexPath(name));
    if (!data) return;

    try {
      const parsed = JSON.parse(new TextDecoder().decode(data)) as SerializedCompoundIndex;
      const compoundIndex = CompoundIndex.deserialize(parsed);

      this.compoundIndexes.set(name, {
        compoundIndex,
        metadata: parsed.metadata,
        dirty: false,
      });
    } catch (error) {
      logger.warn('Failed to load compound index', {
        indexName: name,
        collection: this.collectionName,
        indexType: 'compound',
        error,
      });
    }
  }

  /**
   * Save the list of compound index names
   */
  private async saveCompoundIndexList(): Promise<void> {
    const names = Array.from(this.compoundIndexes.keys());
    const data = JSON.stringify(names);

    await this.storage.put(
      this.getCompoundIndexListPath(),
      new TextEncoder().encode(data)
    );
  }

  /**
   * Load all compound indexes from storage
   */
  private async loadCompoundIndexes(): Promise<void> {
    const listPath = this.getCompoundIndexListPath();
    const listData = await this.storage.get(listPath);

    if (!listData) {
      return;
    }

    try {
      const indexNames = JSON.parse(new TextDecoder().decode(listData)) as string[];

      for (const name of indexNames) {
        await this.loadCompoundIndex(name);
      }
    } catch {
      logger.warn('Compound index list corrupted, starting fresh', {
        collection: this.collectionName,
        indexType: 'compound',
      });
    }
  }

  /**
   * Get the storage path for a compound index
   */
  private getCompoundIndexPath(name: string): string {
    return `${this.dbPath}/${this.collectionName}/_indexes/compound_${name}.json`;
  }

  /**
   * Get the storage path for the compound index list
   */
  private getCompoundIndexListPath(): string {
    return `${this.dbPath}/${this.collectionName}/_indexes/_compound_list.json`;
  }

  // --------------------------------------------------------------------------
  // Geo Index Operations
  // --------------------------------------------------------------------------

  /**
   * Create a geo index (2d or 2dsphere)
   *
   * @param field - Field to index
   * @param type - Index type ('2d' or '2dsphere')
   * @param options - Index options
   * @returns The index name
   *
   * @example
   * ```typescript
   * // Create 2dsphere index on location field
   * await indexManager.createGeoIndex('location', '2dsphere');
   *
   * // Create 2d index with custom bounds
   * await indexManager.createGeoIndex('coordinates', '2d', { min: 0, max: 100 });
   * ```
   */
  async createGeoIndex(
    field: string,
    type: GeoIndexType = '2dsphere',
    options?: IndexOptions & { min?: number; max?: number; bits?: number }
  ): Promise<string> {
    await this.ensureLoaded();

    const name = options?.name || `${field}_${type}`;

    // Check if geo index already exists
    if (this.geoIndexes.has(name)) {
      return name;
    }

    // Create new geo index
    const geoIndex = new GeoIndex(
      name,
      field,
      type,
      {
        min: options?.min,
        max: options?.max,
        bits: options?.bits,
      }
    );

    // Cache the index
    this.geoIndexes.set(name, {
      geoIndex,
      dirty: true,
    });

    // Persist immediately
    await this.saveGeoIndex(name);

    return name;
  }

  /**
   * Drop a geo index
   *
   * @param name - Index name to drop
   * @returns true if index was dropped
   */
  async dropGeoIndex(name: string): Promise<boolean> {
    await this.ensureLoaded();

    if (!this.geoIndexes.has(name)) {
      return false;
    }

    // Remove from cache
    this.geoIndexes.delete(name);

    // Delete from storage
    const indexPath = this.getGeoIndexPath(name);
    try {
      await this.storage.delete(indexPath);
    } catch {
      // Ignore errors if file doesn't exist
    }

    // Update geo index list
    await this.saveGeoIndexList();

    return true;
  }

  /**
   * Get a geo index by name
   *
   * @param name - Index name
   * @returns The geo index or undefined
   */
  async getGeoIndex(name: string): Promise<GeoIndex | undefined> {
    await this.ensureLoaded();
    return this.geoIndexes.get(name)?.geoIndex;
  }

  /**
   * Get geo index by field name
   *
   * @param field - Field name
   * @returns The geo index or undefined
   */
  async getGeoIndexByField(field: string): Promise<GeoIndex | undefined> {
    await this.ensureLoaded();
    for (const cached of this.geoIndexes.values()) {
      if (cached.geoIndex.field === field) {
        return cached.geoIndex;
      }
    }
    return undefined;
  }

  /**
   * Check if collection has a geo index
   */
  async hasGeoIndex(): Promise<boolean> {
    await this.ensureLoaded();
    return this.geoIndexes.size > 0;
  }

  /**
   * List all geo indexes
   *
   * @returns Array of geo index metadata
   */
  async listGeoIndexes(): Promise<GeoIndexMetadata[]> {
    await this.ensureLoaded();
    return Array.from(this.geoIndexes.values()).map((cached) => ({
      name: cached.geoIndex.name,
      field: cached.geoIndex.field,
      type: cached.geoIndex.type,
      min: cached.geoIndex.min,
      max: cached.geoIndex.max,
      bits: cached.geoIndex.bits,
      createdAt: new Date().toISOString(),
    }));
  }

  /**
   * Index a document in all geo indexes
   *
   * @param doc - Document to index
   */
  async indexDocumentGeo(doc: Document): Promise<void> {
    await this.ensureLoaded();

    const docId = this.extractDocId(doc);

    for (const cached of this.geoIndexes.values()) {
      try {
        cached.geoIndex.indexDocument(docId, doc as Record<string, unknown>);
        cached.dirty = true;
      } catch (error) {
        // Log but don't fail for invalid coordinates
        logger.warn('Failed to index document in geo index', {
          indexName: cached.geoIndex.name,
          collection: this.collectionName,
          docId,
          error,
        });
      }
    }
  }

  /**
   * Remove a document from all geo indexes
   *
   * @param docId - Document ID to remove
   */
  async unindexDocumentGeo(docId: string): Promise<void> {
    await this.ensureLoaded();

    for (const cached of this.geoIndexes.values()) {
      cached.geoIndex.unindexDocument(docId);
      cached.dirty = true;
    }
  }

  /**
   * Execute a geo query using an index
   *
   * @param field - Field being queried
   * @param condition - Geo query condition
   * @returns Matching document IDs (with optional distances for $near queries)
   */
  async geoQuery(
    field: string,
    condition: Record<string, unknown>
  ): Promise<GeoDistanceResult[] | string[]> {
    await this.ensureLoaded();

    const geoIndex = await this.getGeoIndexByField(field);
    if (!geoIndex) {
      throw new Error(`No geo index found for field: ${field}`);
    }

    return geoIndex.getMatchingDocIds(condition) as string[];
  }

  /**
   * Save a geo index to storage
   */
  private async saveGeoIndex(name: string): Promise<void> {
    const cached = this.geoIndexes.get(name);
    if (!cached) return;

    const serialized = cached.geoIndex.serialize();
    const data = JSON.stringify(serialized);

    await this.storage.put(
      this.getGeoIndexPath(name),
      new TextEncoder().encode(data)
    );

    // Update geo index list
    await this.saveGeoIndexList();
  }

  /**
   * Load a geo index from storage
   */
  private async loadGeoIndex(name: string): Promise<void> {
    const data = await this.storage.get(this.getGeoIndexPath(name));
    if (!data) return;

    try {
      const parsed = JSON.parse(new TextDecoder().decode(data)) as SerializedGeoIndex;
      const geoIndex = GeoIndex.deserialize(parsed);

      this.geoIndexes.set(name, {
        geoIndex,
        dirty: false,
      });
    } catch (error) {
      logger.warn('Failed to load geo index', {
        indexName: name,
        collection: this.collectionName,
        indexType: 'geo',
        error,
      });
    }
  }

  /**
   * Save the list of geo index names
   */
  private async saveGeoIndexList(): Promise<void> {
    const names = Array.from(this.geoIndexes.keys());
    const data = JSON.stringify(names);

    await this.storage.put(
      this.getGeoIndexListPath(),
      new TextEncoder().encode(data)
    );
  }

  /**
   * Load all geo indexes from storage
   */
  private async loadGeoIndexes(): Promise<void> {
    const listPath = this.getGeoIndexListPath();
    const listData = await this.storage.get(listPath);

    if (!listData) {
      return;
    }

    try {
      const indexNames = JSON.parse(new TextDecoder().decode(listData)) as string[];

      for (const name of indexNames) {
        await this.loadGeoIndex(name);
      }
    } catch {
      logger.warn('Geo index list corrupted, starting fresh', {
        collection: this.collectionName,
        indexType: 'geo',
      });
    }
  }

  /**
   * Get the storage path for a geo index
   */
  private getGeoIndexPath(name: string): string {
    return `${this.dbPath}/${this.collectionName}/_indexes/geo_${name}.json`;
  }

  /**
   * Get the storage path for the geo index list
   */
  private getGeoIndexListPath(): string {
    return `${this.dbPath}/${this.collectionName}/_indexes/_geo_list.json`;
  }

  // --------------------------------------------------------------------------
  // Utilities
  // --------------------------------------------------------------------------

  /**
   * Get the storage path for an index
   */
  private getIndexPath(name: string): string {
    return `${this.dbPath}/${this.collectionName}/_indexes/${name}.json`;
  }

  /**
   * Get the storage path for the index list
   */
  private getIndexListPath(): string {
    return `${this.dbPath}/${this.collectionName}/_indexes/_list.json`;
  }

  /**
   * Extract document ID as string
   */
  private extractDocId(doc: Document): string {
    if (!doc._id) {
      throw new Error('Document must have _id field');
    }
    return typeof doc._id === 'object' && doc._id !== null
      ? doc._id.toString()
      : String(doc._id);
  }
}

// ============================================================================
// Exports
// ============================================================================

export default IndexManager;
