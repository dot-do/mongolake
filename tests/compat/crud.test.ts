/**
 * MongoDB CRUD Compatibility Tests
 *
 * Runs official MongoDB CRUD tests from the unified test format
 * against MongoLake to verify compatibility.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { readFileSync, readdirSync, existsSync, rmSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import {
  UnifiedTestRunner,
  loadTestFile,
  summarizeResults,
  type TestResult,
} from './unified-test-runner.js';

// Get directory path for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Test directory paths
const CRUD_TEST_DIR = join(__dirname, 'crud');

// Collect all results for final summary
const allResults: TestResult[] = [];

describe('MongoDB CRUD Compatibility Tests', () => {
  let runner: UnifiedTestRunner;
  let tempDir: string;

  beforeAll(() => {
    tempDir = `.mongolake-crud-test-${Date.now()}`;
    runner = new UnifiedTestRunner(tempDir);
  });

  afterAll(async () => {
    await runner.cleanup();

    // Clean up temp directory
    if (existsSync(tempDir)) {
      rmSync(tempDir, { recursive: true, force: true });
    }

    // Print summary
    const summary = summarizeResults(allResults);
    console.log('\n====================================');
    console.log('MongoDB CRUD Compatibility Test Summary');
    console.log('====================================');
    console.log(`Total: ${summary.total}`);
    console.log(`Passed: ${summary.passed}`);
    console.log(`Failed: ${summary.failed}`);
    console.log(`Skipped: ${summary.skipped}`);

    if (summary.failed > 0) {
      console.log('\nFailed Tests:');
      for (const result of summary.results.filter((r) => !r.passed)) {
        console.log(`  - ${result.testFile}: ${result.testCase}`);
        console.log(`    Error: ${result.error}`);
      }
    }
  });

  // insertOne
  describe('insertOne', () => {
    it('InsertOne with a non-existing document', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'insertOne.json'), 'utf-8')
      );
      const tc = testFile.tests[0];
      const result = await runner.runTestCase(testFile, tc, 'insertOne');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });
  });

  // insertMany
  describe('insertMany', () => {
    it('InsertMany with non-existing documents', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'insertMany.json'), 'utf-8')
      );
      const tc = testFile.tests[0];
      const result = await runner.runTestCase(testFile, tc, 'insertMany');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });
  });

  // find
  describe('find', () => {
    it('Find with filter', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'find.json'), 'utf-8')
      );
      const tc = testFile.tests.find((t) => t.description === 'Find with filter');
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'find');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });

    it('Find with filter, sort, skip, and limit', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'find.json'), 'utf-8')
      );
      const tc = testFile.tests.find(
        (t) => t.description === 'Find with filter, sort, skip, and limit'
      );
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'find');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });

    it('Find with limit and sort', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'find.json'), 'utf-8')
      );
      const tc = testFile.tests.find(
        (t) => t.description === 'Find with limit and sort'
      );
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'find');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });
  });

  // updateOne
  describe('updateOne', () => {
    it('UpdateOne when one document matches', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'updateOne.json'), 'utf-8')
      );
      const tc = testFile.tests.find(
        (t) => t.description === 'UpdateOne when one document matches'
      );
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'updateOne');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });

    it('UpdateOne when no documents match', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'updateOne.json'), 'utf-8')
      );
      const tc = testFile.tests.find(
        (t) => t.description === 'UpdateOne when no documents match'
      );
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'updateOne');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });

    it('UpdateOne with upsert when no documents match', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'updateOne.json'), 'utf-8')
      );
      const tc = testFile.tests.find(
        (t) => t.description === 'UpdateOne with upsert when no documents match'
      );
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'updateOne');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });
  });

  // updateMany
  describe('updateMany', () => {
    it('UpdateMany when many documents match', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'updateMany.json'), 'utf-8')
      );
      const tc = testFile.tests.find(
        (t) => t.description === 'UpdateMany when many documents match'
      );
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'updateMany');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });

    it('UpdateMany when no documents match', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'updateMany.json'), 'utf-8')
      );
      const tc = testFile.tests.find(
        (t) => t.description === 'UpdateMany when no documents match'
      );
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'updateMany');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });
  });

  // deleteOne
  describe('deleteOne', () => {
    it('DeleteOne when one document matches', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'deleteOne.json'), 'utf-8')
      );
      const tc = testFile.tests.find(
        (t) => t.description === 'DeleteOne when one document matches'
      );
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'deleteOne');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });

    it('DeleteOne when no documents match', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'deleteOne.json'), 'utf-8')
      );
      const tc = testFile.tests.find(
        (t) => t.description === 'DeleteOne when no documents match'
      );
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'deleteOne');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });
  });

  // deleteMany
  describe('deleteMany', () => {
    it('DeleteMany when many documents match', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'deleteMany.json'), 'utf-8')
      );
      const tc = testFile.tests.find(
        (t) => t.description === 'DeleteMany when many documents match'
      );
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'deleteMany');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });

    it('DeleteMany when no document matches', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'deleteMany.json'), 'utf-8')
      );
      const tc = testFile.tests.find(
        (t) => t.description === 'DeleteMany when no document matches'
      );
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'deleteMany');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });
  });

  // findOne
  describe('findOne', () => {
    it('FindOne with filter matching one document', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'findOne.json'), 'utf-8')
      );
      const tc = testFile.tests.find(
        (t) => t.description === 'FindOne with filter matching one document'
      );
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'findOne');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });

    it('FindOne with filter matching no documents', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'findOne.json'), 'utf-8')
      );
      const tc = testFile.tests.find(
        (t) => t.description === 'FindOne with filter matching no documents'
      );
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'findOne');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });

    it('FindOne with projection', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'findOne.json'), 'utf-8')
      );
      const tc = testFile.tests.find(
        (t) => t.description === 'FindOne with projection'
      );
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'findOne');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });
  });

  // replaceOne
  describe('replaceOne', () => {
    it('ReplaceOne when one document matches', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'replaceOne.json'), 'utf-8')
      );
      const tc = testFile.tests.find(
        (t) => t.description === 'ReplaceOne when one document matches'
      );
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'replaceOne');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });

    it('ReplaceOne when no documents match', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'replaceOne.json'), 'utf-8')
      );
      const tc = testFile.tests.find(
        (t) => t.description === 'ReplaceOne when no documents match'
      );
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'replaceOne');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });

    it('ReplaceOne with upsert when no documents match', async () => {
      const testFile = loadTestFile(
        readFileSync(join(CRUD_TEST_DIR, 'replaceOne.json'), 'utf-8')
      );
      const tc = testFile.tests.find(
        (t) => t.description === 'ReplaceOne with upsert when no documents match'
      );
      if (!tc) throw new Error('Test case not found');
      const result = await runner.runTestCase(testFile, tc, 'replaceOne');
      allResults.push(result);
      expect(result.passed, result.error).toBe(true);
    });
  });
});
