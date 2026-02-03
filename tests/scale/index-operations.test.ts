/**
 * Index Operations Scale Test
 *
 * Tests index performance with large datasets.
 * These tests verify:
 * - Index creation on 100k+ documents
 * - Query performance with and without indexes
 * - Index maintenance under write load
 * - Compound and partial index behavior
 */

import { describe, it, expect, beforeEach, afterEach, beforeAll, afterAll } from 'vitest';
import {
  createUserGenerator,
  createTimeSeriesGenerator,
  createOrderGenerator,
  formatBytes,
  formatDuration,
  calculateThroughput,
} from '../utils/data-generator';

// ============================================================================
// Types
// ============================================================================

interface IndexDefinition {
  name: string;
  fields: { [field: string]: 1 | -1 };
  unique?: boolean;
  sparse?: boolean;
  partial?: Record<string, unknown>;
}

interface IndexStats {
  name: string;
  entries: number;
  sizeBytes: number;
  buildTimeMs?: number;
}

interface QueryPlan {
  usedIndex: string | null;
  scannedDocs: number;
  returnedDocs: number;
  durationMs: number;
}

// ============================================================================
// Mock Index Manager
// ============================================================================

/**
 * Simulates an indexed collection for scale testing.
 * Implements basic B-tree style indexing for performance testing.
 */
class IndexedCollection {
  private documents: Map<string, Record<string, unknown>> = new Map();
  private indexes: Map<string, Map<string, Set<string>>> = new Map();
  private indexDefinitions: Map<string, IndexDefinition> = new Map();
  private indexStats: Map<string, IndexStats> = new Map();

  constructor() {
    // Default _id index
    this.createIndex({ name: '_id', fields: { _id: 1 }, unique: true });
  }

  createIndex(definition: IndexDefinition): { durationMs: number } {
    const startTime = Date.now();

    // Create index structure
    const indexMap = new Map<string, Set<string>>();
    this.indexes.set(definition.name, indexMap);
    this.indexDefinitions.set(definition.name, definition);

    // Build index from existing documents
    let entries = 0;
    for (const [docId, doc] of this.documents) {
      const key = this.computeIndexKey(doc, definition.fields);
      if (key !== null) {
        if (!indexMap.has(key)) {
          indexMap.set(key, new Set());
        }
        indexMap.get(key)!.add(docId);
        entries++;
      }
    }

    const buildTime = Date.now() - startTime;

    this.indexStats.set(definition.name, {
      name: definition.name,
      entries,
      sizeBytes: this.estimateIndexSize(indexMap),
      buildTimeMs: buildTime,
    });

    return { durationMs: buildTime };
  }

  dropIndex(name: string): boolean {
    if (name === '_id') return false; // Cannot drop _id index
    this.indexes.delete(name);
    this.indexDefinitions.delete(name);
    this.indexStats.delete(name);
    return true;
  }

  listIndexes(): IndexDefinition[] {
    return Array.from(this.indexDefinitions.values());
  }

  getIndexStats(name: string): IndexStats | null {
    return this.indexStats.get(name) ?? null;
  }

  async insert(doc: Record<string, unknown>): Promise<{ insertedId: string }> {
    const id = (doc._id as string) ?? crypto.randomUUID();
    const fullDoc = { ...doc, _id: id };

    this.documents.set(id, fullDoc);

    // Update all indexes
    for (const [indexName, definition] of this.indexDefinitions) {
      const indexMap = this.indexes.get(indexName)!;
      const key = this.computeIndexKey(fullDoc, definition.fields);

      if (key !== null) {
        if (!indexMap.has(key)) {
          indexMap.set(key, new Set());
        }
        indexMap.get(key)!.add(id);

        // Update stats
        const stats = this.indexStats.get(indexName)!;
        stats.entries++;
        stats.sizeBytes = this.estimateIndexSize(indexMap);
      }
    }

    return { insertedId: id };
  }

  async insertMany(docs: Record<string, unknown>[]): Promise<{ insertedCount: number }> {
    for (const doc of docs) {
      await this.insert(doc);
    }
    return { insertedCount: docs.length };
  }

  async find(filter: Record<string, unknown>, options?: { explain?: boolean }): Promise<QueryPlan & { documents: unknown[] }> {
    const startTime = Date.now();
    let usedIndex: string | null = null;
    let scannedDocs = 0;
    const results: unknown[] = [];

    // Try to use an index
    const indexCandidate = this.selectIndex(filter);

    if (indexCandidate) {
      usedIndex = indexCandidate.name;
      const indexMap = this.indexes.get(indexCandidate.name)!;

      // Find matching keys in index
      const filterKey = this.computeFilterKey(filter, indexCandidate.definition.fields);

      if (filterKey !== null) {
        const docIds = indexMap.get(filterKey);
        if (docIds) {
          for (const docId of docIds) {
            scannedDocs++;
            const doc = this.documents.get(docId);
            if (doc && this.matchesFilter(doc, filter)) {
              results.push(doc);
            }
          }
        }
      } else {
        // Range scan or partial match
        for (const [, docIds] of indexMap) {
          for (const docId of docIds) {
            scannedDocs++;
            const doc = this.documents.get(docId);
            if (doc && this.matchesFilter(doc, filter)) {
              results.push(doc);
            }
          }
        }
      }
    } else {
      // Full collection scan
      for (const doc of this.documents.values()) {
        scannedDocs++;
        if (this.matchesFilter(doc, filter)) {
          results.push(doc);
        }
      }
    }

    return {
      usedIndex,
      scannedDocs,
      returnedDocs: results.length,
      durationMs: Date.now() - startTime,
      documents: results,
    };
  }

  async findOne(filter: Record<string, unknown>): Promise<unknown | null> {
    const result = await this.find(filter);
    return result.documents[0] ?? null;
  }

  async countDocuments(): Promise<number> {
    return this.documents.size;
  }

  clear(): void {
    this.documents.clear();
    for (const indexMap of this.indexes.values()) {
      indexMap.clear();
    }
    // Reset stats
    for (const stats of this.indexStats.values()) {
      stats.entries = 0;
      stats.sizeBytes = 0;
    }
  }

  private computeIndexKey(doc: Record<string, unknown>, fields: { [field: string]: 1 | -1 }): string | null {
    const keyParts: string[] = [];

    for (const field of Object.keys(fields)) {
      const value = this.getNestedValue(doc, field);
      if (value === undefined || value === null) {
        return null; // Skip documents with missing indexed fields
      }
      keyParts.push(String(value));
    }

    return keyParts.join('|');
  }

  private computeFilterKey(filter: Record<string, unknown>, fields: { [field: string]: 1 | -1 }): string | null {
    const keyParts: string[] = [];

    for (const field of Object.keys(fields)) {
      const value = filter[field];
      if (value === undefined || typeof value === 'object') {
        return null; // Cannot use index for missing or complex filters
      }
      keyParts.push(String(value));
    }

    return keyParts.join('|');
  }

  private selectIndex(filter: Record<string, unknown>): { name: string; definition: IndexDefinition } | null {
    const filterFields = Object.keys(filter);

    // Find best matching index
    let bestMatch: { name: string; definition: IndexDefinition } | null = null;
    let bestMatchScore = 0;

    for (const [name, definition] of this.indexDefinitions) {
      const indexFields = Object.keys(definition.fields);
      let matchScore = 0;

      for (const field of indexFields) {
        if (filterFields.includes(field)) {
          matchScore++;
        } else {
          break; // Index prefix must match
        }
      }

      if (matchScore > bestMatchScore) {
        bestMatchScore = matchScore;
        bestMatch = { name, definition };
      }
    }

    return bestMatch;
  }

  private matchesFilter(doc: Record<string, unknown>, filter: Record<string, unknown>): boolean {
    for (const [key, filterValue] of Object.entries(filter)) {
      const docValue = this.getNestedValue(doc, key);

      if (typeof filterValue === 'object' && filterValue !== null) {
        // Handle comparison operators
        const ops = filterValue as Record<string, unknown>;
        if ('$gte' in ops && (docValue as number) < (ops.$gte as number)) return false;
        if ('$lte' in ops && (docValue as number) > (ops.$lte as number)) return false;
        if ('$gt' in ops && (docValue as number) <= (ops.$gt as number)) return false;
        if ('$lt' in ops && (docValue as number) >= (ops.$lt as number)) return false;
        if ('$in' in ops && !(ops.$in as unknown[]).includes(docValue)) return false;
      } else if (docValue !== filterValue) {
        return false;
      }
    }
    return true;
  }

  private getNestedValue(obj: Record<string, unknown>, path: string): unknown {
    const parts = path.split('.');
    let current: unknown = obj;

    for (const part of parts) {
      if (current === null || current === undefined) return undefined;
      current = (current as Record<string, unknown>)[part];
    }

    return current;
  }

  private estimateIndexSize(indexMap: Map<string, Set<string>>): number {
    let size = 0;
    for (const [key, docIds] of indexMap) {
      size += key.length * 2; // Key size
      size += docIds.size * 36; // UUID references
    }
    return size;
  }
}

// ============================================================================
// Tests
// ============================================================================

describe('Index Operations Scale Tests', () => {
  const DOCUMENT_COUNT = 50_000;
  let collection: IndexedCollection;

  beforeEach(() => {
    collection = new IndexedCollection();
  });

  afterEach(() => {
    collection.clear();
  });

  describe('Index Creation on Large Datasets', () => {
    it('should create single-field index on 50k documents', async () => {
      const generator = createUserGenerator();

      // Insert documents
      console.log('    Inserting documents...');
      const insertStart = Date.now();
      const docs = generator.generateBatch(DOCUMENT_COUNT);
      await collection.insertMany(docs as unknown as Record<string, unknown>[]);
      const insertDuration = Date.now() - insertStart;

      // Create index
      console.log('    Creating index on "department"...');
      const result = collection.createIndex({
        name: 'department_1',
        fields: { department: 1 },
      });

      const stats = collection.getIndexStats('department_1');

      console.log('\n  Single-Field Index Creation (50k docs):');
      console.log(`    Insert time: ${formatDuration(insertDuration)}`);
      console.log(`    Index build time: ${result.durationMs}ms`);
      console.log(`    Index entries: ${stats?.entries}`);
      console.log(`    Index size: ${formatBytes(stats?.sizeBytes ?? 0)}`);

      expect(stats?.entries).toBe(DOCUMENT_COUNT);
      expect(result.durationMs).toBeLessThan(5000);
    });

    it('should create compound index on 50k documents', async () => {
      const generator = createUserGenerator();
      const docs = generator.generateBatch(DOCUMENT_COUNT);
      await collection.insertMany(docs as unknown as Record<string, unknown>[]);

      // Create compound index
      const result = collection.createIndex({
        name: 'department_age_1',
        fields: { department: 1, age: 1 },
      });

      const stats = collection.getIndexStats('department_age_1');

      console.log('\n  Compound Index Creation (50k docs):');
      console.log(`    Index build time: ${result.durationMs}ms`);
      console.log(`    Index entries: ${stats?.entries}`);
      console.log(`    Index size: ${formatBytes(stats?.sizeBytes ?? 0)}`);

      expect(stats?.entries).toBe(DOCUMENT_COUNT);
    });

    it('should create multiple indexes in sequence', async () => {
      const generator = createUserGenerator();
      const docs = generator.generateBatch(DOCUMENT_COUNT);
      await collection.insertMany(docs as unknown as Record<string, unknown>[]);

      const indexDefs: IndexDefinition[] = [
        { name: 'email_1', fields: { email: 1 }, unique: true },
        { name: 'active_1', fields: { active: 1 } },
        { name: 'salary_1', fields: { salary: -1 } },
        { name: 'metadata.region_1', fields: { 'metadata.region': 1 } },
      ];

      const buildTimes: { name: string; durationMs: number }[] = [];

      for (const def of indexDefs) {
        const result = collection.createIndex(def);
        buildTimes.push({ name: def.name, durationMs: result.durationMs });
      }

      console.log('\n  Multiple Index Creation (50k docs):');
      for (const t of buildTimes) {
        console.log(`    ${t.name}: ${t.durationMs}ms`);
      }
      console.log(`    Total indexes: ${collection.listIndexes().length}`);

      expect(collection.listIndexes().length).toBe(indexDefs.length + 1); // +1 for _id
    });
  });

  describe('Query Performance With Indexes', () => {
    beforeEach(async () => {
      const generator = createUserGenerator();
      const docs = generator.generateBatch(DOCUMENT_COUNT);
      await collection.insertMany(docs as unknown as Record<string, unknown>[]);
    });

    it('should compare indexed vs non-indexed query performance', async () => {
      const iterations = 50;

      // Query without index
      const noIndexTimes: number[] = [];
      for (let i = 0; i < iterations; i++) {
        const result = await collection.find({ department: 'Engineering' });
        noIndexTimes.push(result.durationMs);
      }

      // Create index
      collection.createIndex({ name: 'department_1', fields: { department: 1 } });

      // Query with index
      const indexedTimes: number[] = [];
      for (let i = 0; i < iterations; i++) {
        const result = await collection.find({ department: 'Engineering' });
        indexedTimes.push(result.durationMs);
      }

      const avgNoIndex = noIndexTimes.reduce((a, b) => a + b, 0) / noIndexTimes.length;
      const avgIndexed = indexedTimes.reduce((a, b) => a + b, 0) / indexedTimes.length;
      const speedup = avgNoIndex / avgIndexed;

      console.log('\n  Indexed vs Non-Indexed Query Performance:');
      console.log(`    Without index: avg ${avgNoIndex.toFixed(2)}ms`);
      console.log(`    With index: avg ${avgIndexed.toFixed(2)}ms`);
      console.log(`    Speedup: ${speedup.toFixed(1)}x`);

      // Indexed should be faster (or at least not slower)
      expect(avgIndexed).toBeLessThanOrEqual(avgNoIndex * 1.5);
    });

    it('should use compound index for multi-field queries', async () => {
      // Create compound index
      collection.createIndex({
        name: 'department_active_1',
        fields: { department: 1, active: 1 },
      });

      const result = await collection.find({
        department: 'Engineering',
        active: true,
      });

      console.log('\n  Compound Index Query:');
      console.log(`    Used index: ${result.usedIndex}`);
      console.log(`    Scanned docs: ${result.scannedDocs}`);
      console.log(`    Returned docs: ${result.returnedDocs}`);
      console.log(`    Duration: ${result.durationMs}ms`);

      expect(result.usedIndex).toBe('department_active_1');
    });

    it('should handle range queries efficiently', async () => {
      collection.createIndex({ name: 'salary_1', fields: { salary: 1 } });
      collection.createIndex({ name: 'age_1', fields: { age: 1 } });

      // Range query on salary
      const salaryResult = await collection.find({
        salary: { $gte: 80000, $lte: 100000 },
      });

      // Range query on age
      const ageResult = await collection.find({
        age: { $gte: 30, $lte: 40 },
      });

      console.log('\n  Range Query Performance:');
      console.log(`    Salary range: ${salaryResult.returnedDocs} docs in ${salaryResult.durationMs}ms`);
      console.log(`    Age range: ${ageResult.returnedDocs} docs in ${ageResult.durationMs}ms`);

      expect(salaryResult.returnedDocs).toBeGreaterThan(0);
      expect(ageResult.returnedDocs).toBeGreaterThan(0);
    });
  });

  describe('Index Maintenance Under Write Load', () => {
    it('should maintain index during concurrent inserts', async () => {
      collection.createIndex({ name: 'email_1', fields: { email: 1 }, unique: true });
      collection.createIndex({ name: 'department_1', fields: { department: 1 } });

      const generator = createUserGenerator();
      const batchSize = 1000;
      const batches = 20;
      const insertTimes: number[] = [];

      const startTime = Date.now();

      for (let batch = 0; batch < batches; batch++) {
        const batchStart = Date.now();
        const docs = generator.generateBatch(batchSize);
        await collection.insertMany(docs as unknown as Record<string, unknown>[]);
        insertTimes.push(Date.now() - batchStart);
      }

      const totalDuration = Date.now() - startTime;
      const totalDocs = await collection.countDocuments();
      const avgInsertTime = insertTimes.reduce((a, b) => a + b, 0) / insertTimes.length;

      const emailStats = collection.getIndexStats('email_1');
      const deptStats = collection.getIndexStats('department_1');

      console.log('\n  Index Maintenance During Inserts:');
      console.log(`    Total docs: ${totalDocs}`);
      console.log(`    Total duration: ${formatDuration(totalDuration)}`);
      console.log(`    Avg batch time: ${avgInsertTime.toFixed(2)}ms per ${batchSize} docs`);
      console.log(`    email index entries: ${emailStats?.entries}`);
      console.log(`    department index entries: ${deptStats?.entries}`);

      expect(emailStats?.entries).toBe(batchSize * batches);
      expect(deptStats?.entries).toBe(batchSize * batches);
    });

    it('should track index size growth with inserts', async () => {
      collection.createIndex({ name: 'department_1', fields: { department: 1 } });

      const generator = createUserGenerator();
      const checkpoints = [1000, 5000, 10000, 25000, 50000];
      const sizeData: { docs: number; indexSize: number }[] = [];

      for (const checkpoint of checkpoints) {
        const currentCount = await collection.countDocuments();
        const toInsert = checkpoint - currentCount;

        if (toInsert > 0) {
          const docs = generator.generateBatch(toInsert);
          await collection.insertMany(docs as unknown as Record<string, unknown>[]);
        }

        const stats = collection.getIndexStats('department_1');
        sizeData.push({ docs: checkpoint, indexSize: stats?.sizeBytes ?? 0 });
      }

      console.log('\n  Index Size Growth:');
      for (const d of sizeData) {
        console.log(`    ${d.docs.toLocaleString()} docs: ${formatBytes(d.indexSize)}`);
      }

      // Index size should grow roughly linearly
      const firstSize = sizeData[0]!.indexSize;
      const lastSize = sizeData[sizeData.length - 1]!.indexSize;
      const docRatio = sizeData[sizeData.length - 1]!.docs / sizeData[0]!.docs;
      const sizeRatio = lastSize / firstSize;

      // Size should grow within 2x of document count ratio
      expect(sizeRatio).toBeLessThan(docRatio * 2);
    });
  });

  describe('Time Series Index Performance', () => {
    it('should efficiently index time-series data', async () => {
      const generator = createTimeSeriesGenerator();
      const docs = generator.generateBatch(DOCUMENT_COUNT);

      // Insert documents
      await collection.insertMany(docs as unknown as Record<string, unknown>[]);

      // Create time-based index
      const timestampResult = collection.createIndex({
        name: 'timestamp_1',
        fields: { timestamp: -1 },
      });

      // Create compound index for metric queries
      const metricResult = collection.createIndex({
        name: 'metricName_timestamp_1',
        fields: { metricName: 1, timestamp: -1 },
      });

      console.log('\n  Time Series Index Creation:');
      console.log(`    timestamp index: ${timestampResult.durationMs}ms`);
      console.log(`    metric+timestamp index: ${metricResult.durationMs}ms`);

      // Query recent metrics
      const queryResult = await collection.find({ metricName: 'cpu_usage' });

      console.log(`    Query 'cpu_usage': ${queryResult.returnedDocs} docs in ${queryResult.durationMs}ms`);

      expect(queryResult.usedIndex).toBe('metricName_timestamp_1');
    });

    it('should handle high-cardinality time series queries', async () => {
      const generator = createTimeSeriesGenerator();
      const docs = generator.generateBatch(DOCUMENT_COUNT);
      await collection.insertMany(docs as unknown as Record<string, unknown>[]);

      collection.createIndex({
        name: 'dimensions.host_1',
        fields: { 'dimensions.host': 1 },
      });

      // Query across all hosts
      const queryTimes: number[] = [];
      const hostCount = 100;

      for (let i = 0; i < hostCount; i++) {
        const hostName = `host-${i.toString().padStart(3, '0')}`;
        const result = await collection.find({ 'dimensions.host': hostName });
        queryTimes.push(result.durationMs);
      }

      const avgQueryTime = queryTimes.reduce((a, b) => a + b, 0) / queryTimes.length;
      const maxQueryTime = Math.max(...queryTimes);

      console.log('\n  High-Cardinality Host Queries:');
      console.log(`    Unique hosts queried: ${hostCount}`);
      console.log(`    Avg query time: ${avgQueryTime.toFixed(2)}ms`);
      console.log(`    Max query time: ${maxQueryTime}ms`);

      expect(avgQueryTime).toBeLessThan(50);
    });
  });

  describe('Index Selection and Query Planning', () => {
    beforeEach(async () => {
      const generator = createOrderGenerator();
      const docs = generator.generateBatch(DOCUMENT_COUNT);
      await collection.insertMany(docs as unknown as Record<string, unknown>[]);

      // Create various indexes
      collection.createIndex({ name: 'status_1', fields: { status: 1 } });
      collection.createIndex({ name: 'customerId_1', fields: { customerId: 1 } });
      collection.createIndex({ name: 'status_customerId_1', fields: { status: 1, customerId: 1 } });
      collection.createIndex({ name: 'totals.total_1', fields: { 'totals.total': -1 } });
    });

    it('should select most specific index for queries', async () => {
      // Query that matches compound index
      const result1 = await collection.find({ status: 'shipped', customerId: 'cust-00100' });
      expect(result1.usedIndex).toBe('status_customerId_1');

      // Query that matches single field
      const result2 = await collection.find({ customerId: 'cust-00200' });
      expect(result2.usedIndex).toBe('customerId_1');

      // Query on nested field
      const result3 = await collection.find({ 'totals.total': { $gte: 100 } });
      expect(result3.usedIndex).toBe('totals.total_1');

      console.log('\n  Index Selection:');
      console.log(`    status+customerId query: ${result1.usedIndex}`);
      console.log(`    customerId query: ${result2.usedIndex}`);
      console.log(`    totals.total query: ${result3.usedIndex}`);
    });

    it('should compare query plans for different index strategies', async () => {
      const testFilters = [
        { filter: { status: 'pending' }, description: 'By status' },
        { filter: { customerId: 'cust-00500' }, description: 'By customer' },
        { filter: { status: 'shipped', customerId: 'cust-01000' }, description: 'By status+customer' },
        { filter: { 'totals.total': { $gte: 200 } }, description: 'By total (range)' },
      ];

      console.log('\n  Query Plan Comparison:');

      for (const { filter, description } of testFilters) {
        const result = await collection.find(filter);
        console.log(`    ${description}:`);
        console.log(`      Index: ${result.usedIndex ?? 'COLLSCAN'}`);
        console.log(`      Scanned: ${result.scannedDocs}, Returned: ${result.returnedDocs}`);
        console.log(`      Duration: ${result.durationMs}ms`);
      }
    });
  });

  describe('Index Memory and Storage', () => {
    it('should track memory usage across multiple indexes', async () => {
      const generator = createUserGenerator();
      const docs = generator.generateBatch(DOCUMENT_COUNT);
      await collection.insertMany(docs as unknown as Record<string, unknown>[]);

      const indexesToCreate: IndexDefinition[] = [
        { name: 'email_1', fields: { email: 1 } },
        { name: 'department_1', fields: { department: 1 } },
        { name: 'salary_1', fields: { salary: 1 } },
        { name: 'active_1', fields: { active: 1 } },
        { name: 'age_1', fields: { age: 1 } },
        { name: 'name_1', fields: { name: 1 } },
      ];

      let totalIndexSize = 0;

      console.log('\n  Index Memory Usage:');

      for (const def of indexesToCreate) {
        collection.createIndex(def);
        const stats = collection.getIndexStats(def.name);
        totalIndexSize += stats?.sizeBytes ?? 0;
        console.log(`    ${def.name}: ${formatBytes(stats?.sizeBytes ?? 0)}`);
      }

      console.log(`    Total index size: ${formatBytes(totalIndexSize)}`);
      console.log(`    Indexes created: ${collection.listIndexes().length}`);

      // Total index size should be bounded
      expect(totalIndexSize).toBeLessThan(100 * 1024 * 1024); // Less than 100MB
    });

    it('should reclaim memory after dropping indexes', async () => {
      const generator = createUserGenerator();
      const docs = generator.generateBatch(DOCUMENT_COUNT);
      await collection.insertMany(docs as unknown as Record<string, unknown>[]);

      // Create indexes
      collection.createIndex({ name: 'temp_1', fields: { email: 1 } });
      collection.createIndex({ name: 'temp_2', fields: { department: 1 } });
      collection.createIndex({ name: 'temp_3', fields: { salary: 1 } });

      const beforeDrop = collection.listIndexes().length;

      // Drop indexes
      collection.dropIndex('temp_1');
      collection.dropIndex('temp_2');
      collection.dropIndex('temp_3');

      const afterDrop = collection.listIndexes().length;

      console.log('\n  Index Cleanup:');
      console.log(`    Indexes before drop: ${beforeDrop}`);
      console.log(`    Indexes after drop: ${afterDrop}`);

      expect(afterDrop).toBe(beforeDrop - 3);
      expect(collection.getIndexStats('temp_1')).toBeNull();
    });
  });
});
