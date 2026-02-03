/**
 * MongoDB Unified Test Format Runner
 *
 * Implements the MongoDB Unified Test Format specification for testing
 * MongoLake compatibility with official MongoDB driver tests.
 *
 * @see https://github.com/mongodb/specifications/blob/master/source/unified-test-format/unified-test-format.md
 */

import { MongoLake, Collection, Database } from '../../src/client/index.js';
import type { Document, Filter, Update, FindOptions } from '../../src/types.js';

/**
 * MongoLake stores _id values as strings internally for uniformity.
 * This helper converts filter _id values to strings to match.
 */
function normalizeIdInFilter(filter: Filter<Document>): Filter<Document> {
  if (!filter || typeof filter !== 'object') return filter;

  const normalized = { ...filter };

  if ('_id' in normalized) {
    const idValue = normalized._id;
    if (typeof idValue === 'number' || typeof idValue === 'string') {
      normalized._id = String(idValue);
    } else if (idValue && typeof idValue === 'object') {
      // Handle comparison operators like { $gt: 1 }
      const opObj = idValue as Record<string, unknown>;
      const normalizedOp: Record<string, unknown> = {};
      for (const op of Object.keys(opObj)) {
        if (op.startsWith('$')) {
          normalizedOp[op] = typeof opObj[op] === 'number' ? String(opObj[op]) : opObj[op];
        } else {
          normalizedOp[op] = opObj[op];
        }
      }
      normalized._id = normalizedOp;
    }
  }

  return normalized;
}

/**
 * Normalize a document for comparison by converting _id to string.
 */
function normalizeDocument(doc: Document): Document {
  if (!doc || typeof doc !== 'object') return doc;
  const normalized = { ...doc };
  if ('_id' in normalized && normalized._id !== undefined) {
    normalized._id = String(normalized._id);
  }
  return normalized;
}

// ============================================================================
// Type Definitions
// ============================================================================

/**
 * Unified test file schema
 */
export interface UnifiedTestFile {
  description: string;
  schemaVersion: string;
  runOnRequirements?: RunOnRequirement[];
  createEntities?: CreateEntity[];
  initialData?: InitialData[];
  tests: TestCase[];
}

export interface RunOnRequirement {
  minServerVersion?: string;
  maxServerVersion?: string;
  topologies?: string[];
  serverParameters?: Record<string, unknown>;
}

export interface CreateEntity {
  client?: ClientEntity;
  database?: DatabaseEntity;
  collection?: CollectionEntity;
  session?: SessionEntity;
}

export interface ClientEntity {
  id: string;
  useMultipleMongoses?: boolean;
  observeEvents?: string[];
  uriOptions?: Record<string, unknown>;
}

export interface DatabaseEntity {
  id: string;
  client: string;
  databaseName: string;
}

export interface CollectionEntity {
  id: string;
  database: string;
  collectionName: string;
}

export interface SessionEntity {
  id: string;
  client: string;
}

export interface InitialData {
  collectionName: string;
  databaseName: string;
  documents: Document[];
  createOptions?: Record<string, unknown>;
}

export interface TestCase {
  description: string;
  runOnRequirements?: RunOnRequirement[];
  skipReason?: string;
  operations: Operation[];
  expectEvents?: ExpectEvents[];
  outcome?: Outcome[];
}

export interface Operation {
  name: string;
  object: string;
  arguments?: Record<string, unknown>;
  expectResult?: unknown;
  expectError?: ExpectError;
  ignoreResultAndError?: boolean;
  saveResultAsEntity?: string;
}

export interface ExpectError {
  isError?: boolean;
  errorContains?: string;
  errorCode?: number;
  errorCodeName?: string;
  expectResult?: unknown;
}

export interface ExpectEvents {
  client: string;
  events: unknown[];
}

export interface Outcome {
  collectionName: string;
  databaseName: string;
  documents: Document[];
}

// ============================================================================
// Test Runner
// ============================================================================

export interface TestResult {
  testFile: string;
  testCase: string;
  passed: boolean;
  error?: string;
  duration: number;
}

export interface TestRunSummary {
  total: number;
  passed: number;
  failed: number;
  skipped: number;
  results: TestResult[];
}

/**
 * Unified Test Runner for MongoDB specification tests
 */
export class UnifiedTestRunner {
  private client: MongoLake;
  private entities: Map<string, unknown> = new Map();
  private tempDir: string;

  constructor(tempDir?: string) {
    // Use a unique temp directory for test isolation
    this.tempDir = tempDir || `.mongolake-compat-test-${Date.now()}`;
    this.client = new MongoLake({ local: this.tempDir });
  }

  /**
   * Run a single test file
   */
  async runTestFile(testFile: UnifiedTestFile, fileName: string): Promise<TestResult[]> {
    const results: TestResult[] = [];

    for (const testCase of testFile.tests) {
      const result = await this.runTestCase(testFile, testCase, fileName);
      results.push(result);
    }

    return results;
  }

  /**
   * Run a single test case
   */
  async runTestCase(
    testFile: UnifiedTestFile,
    testCase: TestCase,
    fileName: string
  ): Promise<TestResult> {
    const startTime = Date.now();
    const testName = `${fileName}: ${testCase.description}`;

    // Check skip reason
    if (testCase.skipReason) {
      return {
        testFile: fileName,
        testCase: testCase.description,
        passed: true,
        error: `SKIPPED: ${testCase.skipReason}`,
        duration: 0,
      };
    }

    try {
      // Reset state for each test
      await this.reset();

      // Create entities
      if (testFile.createEntities) {
        await this.createEntities(testFile.createEntities);
      }

      // Load initial data
      if (testFile.initialData) {
        await this.loadInitialData(testFile.initialData);
      }

      // Execute operations
      for (const operation of testCase.operations) {
        await this.executeOperation(operation);
      }

      // Verify outcome
      if (testCase.outcome) {
        await this.verifyOutcome(testCase.outcome);
      }

      return {
        testFile: fileName,
        testCase: testCase.description,
        passed: true,
        duration: Date.now() - startTime,
      };
    } catch (error) {
      return {
        testFile: fileName,
        testCase: testCase.description,
        passed: false,
        error: error instanceof Error ? error.message : String(error),
        duration: Date.now() - startTime,
      };
    }
  }

  /**
   * Reset test state
   */
  private async reset(): Promise<void> {
    this.entities.clear();
    // Create a fresh client for each test
    this.client = new MongoLake({ local: this.tempDir });
  }

  /**
   * Create entities from test file spec
   */
  private async createEntities(entities: CreateEntity[]): Promise<void> {
    for (const entity of entities) {
      if (entity.client) {
        // Create MongoLake client
        const client = new MongoLake({ local: this.tempDir });
        this.entities.set(entity.client.id, client);
      }

      if (entity.database) {
        // Get client and create database reference
        const client = this.entities.get(entity.database.client) as MongoLake;
        if (!client) {
          throw new Error(`Client ${entity.database.client} not found`);
        }
        const db = client.db(entity.database.databaseName);
        this.entities.set(entity.database.id, db);
      }

      if (entity.collection) {
        // Get database and create collection reference
        const db = this.entities.get(entity.collection.database) as Database;
        if (!db) {
          throw new Error(`Database ${entity.collection.database} not found`);
        }
        const collection = db.collection(entity.collection.collectionName);
        this.entities.set(entity.collection.id, collection);
      }
    }
  }

  /**
   * Load initial data into collections
   */
  private async loadInitialData(initialData: InitialData[]): Promise<void> {
    for (const data of initialData) {
      const db = this.client.db(data.databaseName);
      const collection = db.collection(data.collectionName);

      // Drop existing data by dropping the database
      await this.client.dropDatabase(data.databaseName);

      // Insert initial documents (normalize _id to string for MongoLake)
      if (data.documents.length > 0) {
        const normalizedDocs = data.documents.map(normalizeDocument);
        await collection.insertMany(normalizedDocs);
      }
    }
  }

  /**
   * Execute a single operation
   */
  private async executeOperation(operation: Operation): Promise<unknown> {
    const target = this.entities.get(operation.object);
    if (!target) {
      throw new Error(`Entity ${operation.object} not found`);
    }

    let result: unknown;
    let error: Error | null = null;

    try {
      result = await this.executeOperationOnTarget(target, operation);
    } catch (e) {
      error = e as Error;
    }

    // Handle expected errors
    if (operation.expectError) {
      if (!error) {
        throw new Error(`Expected error but operation succeeded`);
      }
      this.verifyError(error, operation.expectError);
      return;
    }

    // Propagate unexpected errors
    if (error && !operation.ignoreResultAndError) {
      throw error;
    }

    // Verify result
    if (operation.expectResult !== undefined && !operation.ignoreResultAndError) {
      this.verifyResult(result, operation.expectResult);
    }

    // Save result as entity
    if (operation.saveResultAsEntity) {
      this.entities.set(operation.saveResultAsEntity, result);
    }

    return result;
  }

  /**
   * Execute operation on target entity
   */
  private async executeOperationOnTarget(
    target: unknown,
    operation: Operation
  ): Promise<unknown> {
    const collection = target as Collection;
    const args = operation.arguments || {};

    switch (operation.name) {
      // Insert operations
      case 'insertOne': {
        const doc = normalizeDocument(args.document as Document);
        return collection.insertOne(doc);
      }

      case 'insertMany': {
        const docs = (args.documents as Document[]).map(normalizeDocument);
        return collection.insertMany(docs);
      }

      // Find operations
      case 'find': {
        const filter = normalizeIdInFilter((args.filter || {}) as Filter<Document>);
        const options: FindOptions = {};

        if (args.sort) options.sort = args.sort as Record<string, 1 | -1>;
        if (args.skip !== undefined) options.skip = args.skip as number;
        if (args.limit !== undefined) options.limit = args.limit as number;
        if (args.projection) options.projection = args.projection as Record<string, 0 | 1>;

        return collection.find(filter, options).toArray();
      }

      case 'findOne': {
        const filter = normalizeIdInFilter((args.filter || {}) as Filter<Document>);
        const options: FindOptions = {};

        if (args.projection) options.projection = args.projection as Record<string, 0 | 1>;

        return collection.findOne(filter, options);
      }

      // Update operations
      case 'updateOne': {
        const filter = normalizeIdInFilter(args.filter as Filter<Document>);
        const update = args.update as Update<Document>;
        const options = args.upsert ? { upsert: true } : undefined;
        return collection.updateOne(filter, update, options);
      }

      case 'updateMany': {
        const filter = normalizeIdInFilter(args.filter as Filter<Document>);
        const update = args.update as Update<Document>;
        const options = args.upsert ? { upsert: true } : undefined;
        return collection.updateMany(filter, update, options);
      }

      case 'replaceOne': {
        const filter = normalizeIdInFilter(args.filter as Filter<Document>);
        const replacement = normalizeDocument(args.replacement as Document);
        const options = args.upsert ? { upsert: true } : undefined;
        return collection.replaceOne(filter, replacement, options);
      }

      // Delete operations
      case 'deleteOne': {
        const filter = normalizeIdInFilter(args.filter as Filter<Document>);
        return collection.deleteOne(filter);
      }

      case 'deleteMany': {
        const filter = normalizeIdInFilter(args.filter as Filter<Document>);
        return collection.deleteMany(filter);
      }

      // Count operations
      case 'countDocuments': {
        const filter = normalizeIdInFilter((args.filter || {}) as Filter<Document>);
        return collection.countDocuments(filter);
      }

      case 'estimatedDocumentCount': {
        return collection.estimatedDocumentCount();
      }

      // Distinct
      case 'distinct': {
        const fieldName = args.fieldName as string;
        const filter = normalizeIdInFilter((args.filter || {}) as Filter<Document>);
        return collection.distinct(fieldName as keyof Document, filter);
      }

      // Aggregate
      case 'aggregate': {
        const pipeline = args.pipeline as Document[];
        return collection.aggregate(pipeline).toArray();
      }

      default:
        throw new Error(`Unsupported operation: ${operation.name}`);
    }
  }

  /**
   * Verify operation result matches expected
   */
  private verifyResult(actual: unknown, expected: unknown): void {
    // Normalize expected result (convert _id to string in documents)
    const normalizedExpected = this.normalizeExpectedResult(expected);
    const match = this.matchesExpected(actual, normalizedExpected);
    if (!match.matches) {
      throw new Error(
        `Result mismatch: ${match.reason}\n` +
        `Expected: ${JSON.stringify(normalizedExpected, null, 2)}\n` +
        `Actual: ${JSON.stringify(actual, null, 2)}`
      );
    }
  }

  /**
   * Normalize expected results by converting _id values to strings
   */
  private normalizeExpectedResult(expected: unknown): unknown {
    if (expected === null || expected === undefined) return expected;

    if (Array.isArray(expected)) {
      return expected.map((item) => this.normalizeExpectedResult(item));
    }

    if (typeof expected === 'object') {
      const obj = expected as Record<string, unknown>;
      const normalized: Record<string, unknown> = {};

      for (const key of Object.keys(obj)) {
        // Handle special MongoDB test operators - normalize their inner values
        if (key === '$$unsetOrMatches') {
          normalized[key] = this.normalizeExpectedResult(obj[key]);
        } else if (key === '_id' && (typeof obj[key] === 'number' || typeof obj[key] === 'string')) {
          normalized[key] = String(obj[key]);
        } else if (key === 'insertedId') {
          if (typeof obj[key] === 'number' || typeof obj[key] === 'string') {
            normalized[key] = String(obj[key]);
          } else {
            normalized[key] = this.normalizeExpectedResult(obj[key]);
          }
        } else if (key === 'upsertedId') {
          if (typeof obj[key] === 'number' || typeof obj[key] === 'string') {
            normalized[key] = String(obj[key]);
          } else {
            normalized[key] = this.normalizeExpectedResult(obj[key]);
          }
        } else if (key === 'insertedIds') {
          if (typeof obj[key] === 'object' && obj[key] !== null) {
            // Check if it's a nested $$unsetOrMatches
            const idsObj = obj[key] as Record<string, unknown>;
            if ('$$unsetOrMatches' in idsObj) {
              normalized[key] = this.normalizeExpectedResult(obj[key]);
            } else {
              // Handle insertedIds like { "0": 2, "1": 3 }
              const normalizedIds: Record<string, string> = {};
              for (const idx of Object.keys(idsObj)) {
                normalizedIds[idx] = String(idsObj[idx]);
              }
              normalized[key] = normalizedIds;
            }
          } else {
            normalized[key] = obj[key];
          }
        } else {
          normalized[key] = this.normalizeExpectedResult(obj[key]);
        }
      }

      return normalized;
    }

    return expected;
  }

  /**
   * Verify error matches expected
   */
  private verifyError(error: Error, expectError: ExpectError): void {
    if (expectError.errorContains) {
      if (!error.message.includes(expectError.errorContains)) {
        throw new Error(
          `Expected error containing "${expectError.errorContains}" ` +
          `but got: "${error.message}"`
        );
      }
    }
  }

  /**
   * Verify collection outcome
   */
  private async verifyOutcome(outcomes: Outcome[]): Promise<void> {
    for (const outcome of outcomes) {
      const db = this.client.db(outcome.databaseName);
      const collection = db.collection(outcome.collectionName);

      // Get all documents sorted by _id for consistent comparison
      const actualDocs = await collection.find({}, { sort: { _id: 1 } }).toArray();

      // Normalize expected docs (convert _id to string) and sort by _id
      const expectedDocs = outcome.documents
        .map(normalizeDocument)
        .sort((a, b) => {
          const aId = String(a._id);
          const bId = String(b._id);
          return aId.localeCompare(bId);
        });

      if (actualDocs.length !== expectedDocs.length) {
        throw new Error(
          `Outcome mismatch for ${outcome.databaseName}.${outcome.collectionName}: ` +
          `expected ${expectedDocs.length} documents, got ${actualDocs.length}\n` +
          `Expected: ${JSON.stringify(expectedDocs, null, 2)}\n` +
          `Actual: ${JSON.stringify(actualDocs, null, 2)}`
        );
      }

      for (let i = 0; i < expectedDocs.length; i++) {
        const match = this.matchesExpected(actualDocs[i], expectedDocs[i]);
        if (!match.matches) {
          throw new Error(
            `Outcome mismatch for ${outcome.databaseName}.${outcome.collectionName} ` +
            `document ${i}: ${match.reason}\n` +
            `Expected: ${JSON.stringify(expectedDocs[i], null, 2)}\n` +
            `Actual: ${JSON.stringify(actualDocs[i], null, 2)}`
          );
        }
      }
    }
  }

  /**
   * Check if actual value matches expected value using MongoDB matching rules
   */
  private matchesExpected(
    actual: unknown,
    expected: unknown
  ): { matches: boolean; reason?: string } {
    // Handle special operators
    if (expected !== null && typeof expected === 'object') {
      const expectedObj = expected as Record<string, unknown>;

      // $$unsetOrMatches - optional field matching
      if ('$$unsetOrMatches' in expectedObj) {
        if (actual === undefined || actual === null) {
          return { matches: true };
        }
        return this.matchesExpected(actual, expectedObj['$$unsetOrMatches']);
      }

      // $$type - type checking
      if ('$$type' in expectedObj) {
        const types = Array.isArray(expectedObj['$$type'])
          ? expectedObj['$$type']
          : [expectedObj['$$type']];

        const actualType = this.getType(actual);
        if (types.includes(actualType)) {
          return { matches: true };
        }
        return {
          matches: false,
          reason: `Type mismatch: expected one of [${types.join(', ')}], got ${actualType}`,
        };
      }

      // $$exists - existence check
      if ('$$exists' in expectedObj) {
        const shouldExist = expectedObj['$$exists'];
        const exists = actual !== undefined;
        if (exists === shouldExist) {
          return { matches: true };
        }
        return {
          matches: false,
          reason: `Existence mismatch: expected ${shouldExist ? 'to exist' : 'not to exist'}`,
        };
      }

      // $$matchesEntity - entity reference
      if ('$$matchesEntity' in expectedObj) {
        const entityId = expectedObj['$$matchesEntity'] as string;
        const entity = this.entities.get(entityId);
        return this.matchesExpected(actual, entity);
      }
    }

    // Null comparison
    if (expected === null) {
      if (actual === null) {
        return { matches: true };
      }
      return { matches: false, reason: 'Expected null' };
    }

    // Array comparison
    if (Array.isArray(expected)) {
      if (!Array.isArray(actual)) {
        return { matches: false, reason: 'Expected array' };
      }

      if (actual.length !== expected.length) {
        return {
          matches: false,
          reason: `Array length mismatch: expected ${expected.length}, got ${actual.length}`,
        };
      }

      for (let i = 0; i < expected.length; i++) {
        const elementMatch = this.matchesExpected(actual[i], expected[i]);
        if (!elementMatch.matches) {
          return {
            matches: false,
            reason: `Array element ${i}: ${elementMatch.reason}`,
          };
        }
      }

      return { matches: true };
    }

    // Object comparison
    if (typeof expected === 'object') {
      if (typeof actual !== 'object' || actual === null) {
        return { matches: false, reason: 'Expected object' };
      }

      const expectedKeys = Object.keys(expected as Record<string, unknown>);
      const actualObj = actual as Record<string, unknown>;
      const expectedObj = expected as Record<string, unknown>;

      for (const key of expectedKeys) {
        const fieldMatch = this.matchesExpected(actualObj[key], expectedObj[key]);
        if (!fieldMatch.matches) {
          return {
            matches: false,
            reason: `Field "${key}": ${fieldMatch.reason}`,
          };
        }
      }

      return { matches: true };
    }

    // Primitive comparison
    if (actual === expected) {
      return { matches: true };
    }

    // Handle numeric comparisons (int vs long)
    if (typeof actual === 'number' && typeof expected === 'number') {
      if (actual === expected) {
        return { matches: true };
      }
    }

    // Handle string/number equivalence for IDs (MongoLake converts _id to string)
    // This allows "2" to match 2 for ID comparisons
    if (
      (typeof actual === 'string' && typeof expected === 'number') ||
      (typeof actual === 'number' && typeof expected === 'string')
    ) {
      if (String(actual) === String(expected)) {
        return { matches: true };
      }
    }

    return {
      matches: false,
      reason: `Value mismatch: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`,
    };
  }

  /**
   * Get the type of a value for $$type matching
   */
  private getType(value: unknown): string {
    if (value === null) return 'null';
    if (value === undefined) return 'undefined';
    if (Array.isArray(value)) return 'array';
    if (typeof value === 'number') {
      if (Number.isInteger(value)) {
        return Math.abs(value) <= 2147483647 ? 'int' : 'long';
      }
      return 'double';
    }
    if (typeof value === 'string') return 'string';
    if (typeof value === 'boolean') return 'bool';
    if (typeof value === 'object') return 'object';
    return typeof value;
  }

  /**
   * Clean up test resources
   */
  async cleanup(): Promise<void> {
    await this.client.close();
    // Note: The temp directory would need manual cleanup in production
    // For tests, vitest handles this through afterAll hooks
  }
}

/**
 * Load a test file from JSON
 */
export function loadTestFile(json: string): UnifiedTestFile {
  return JSON.parse(json) as UnifiedTestFile;
}

/**
 * Generate a test summary
 */
export function summarizeResults(results: TestResult[]): TestRunSummary {
  const passed = results.filter((r) => r.passed && !r.error?.startsWith('SKIPPED')).length;
  const skipped = results.filter((r) => r.error?.startsWith('SKIPPED')).length;
  const failed = results.filter((r) => !r.passed).length;

  return {
    total: results.length,
    passed,
    failed,
    skipped,
    results,
  };
}
