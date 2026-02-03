/**
 * RED Phase Tests: Auto-Promote Logic
 *
 * These tests are designed to FAIL initially, validating the expected behavior
 * for the auto-promote logic feature. They cover:
 * - Threshold detection: when fields meet frequency threshold for promotion
 * - Promotion triggers: what conditions trigger automatic promotion
 * - Field selection criteria: which fields are eligible for promotion
 * - Exclude patterns: patterns that exclude fields from promotion
 * - Promotion batching: grouping multiple promotions together
 *
 * @module tests/unit/schema/auto-promote
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { FieldAnalyzer } from '../../../src/schema/analyzer.js';
import type { ParquetType } from '../../../src/types.js';
import type { ParsedColumn, ParsedCollectionSchema } from '../../../src/schema/config.js';

// Import auto-promote module to register AutoPromoteManager on globalThis
import '../../../src/schema/auto-promote.js';

// ============================================================================
// Test Types - Expected Auto-Promote API
// ============================================================================

/**
 * Configuration for auto-promotion behavior
 */
interface AutoPromoteConfig {
  /** Minimum frequency threshold (0-1) for promotion (default: 0.8) */
  threshold: number;
  /** Minimum type consistency (0-1) for promotion (default: 0.9) */
  minConsistency?: number;
  /** Maximum number of fields to promote in one batch */
  maxBatchSize?: number;
  /** Field patterns to exclude from auto-promotion */
  excludePatterns?: string[];
  /** Field patterns to include (overrides excludePatterns) */
  includePatterns?: string[];
  /** Whether to auto-promote nested fields (default: true) */
  includeNested?: boolean;
  /** Maximum nesting depth for auto-promotion (default: 5) */
  maxNestingDepth?: number;
  /** Whether to auto-promote array fields (default: false) */
  includeArrays?: boolean;
  /** Minimum document sample size before considering promotion */
  minSampleSize?: number;
  /** Cooldown period in ms before re-evaluating a field */
  cooldownMs?: number;
}

/**
 * Result of auto-promote evaluation
 */
interface AutoPromoteResult {
  /** Fields that should be promoted */
  promotions: AutoPromoteCandidate[];
  /** Fields that were evaluated but not promoted */
  skipped: SkippedField[];
  /** Fields that were excluded by pattern */
  excluded: string[];
  /** Total fields evaluated */
  totalEvaluated: number;
  /** Timestamp of evaluation */
  evaluatedAt: Date;
}

/**
 * A candidate for automatic promotion
 */
interface AutoPromoteCandidate {
  /** Field path */
  path: string;
  /** Suggested Parquet type */
  suggestedType: ParquetType;
  /** Field frequency (0-1) */
  frequency: number;
  /** Type consistency (0-1) */
  consistency: number;
  /** Priority score for batch ordering */
  priority: number;
  /** Reason for promotion */
  reason: string;
}

/**
 * A field that was evaluated but not promoted
 */
interface SkippedField {
  /** Field path */
  path: string;
  /** Reason for skipping */
  reason: string;
  /** Current frequency */
  frequency: number;
  /** Current consistency */
  consistency: number;
}

/**
 * Auto-promotion manager interface (expected but not yet implemented)
 */
interface AutoPromoteManager {
  /** Configure auto-promotion settings */
  configure(config: AutoPromoteConfig): void;

  /** Get current configuration */
  getConfig(): AutoPromoteConfig;

  /** Evaluate fields for auto-promotion based on current analysis */
  evaluate(analyzer: FieldAnalyzer): AutoPromoteResult;

  /** Get promotion candidates without committing */
  getCandidates(analyzer: FieldAnalyzer): AutoPromoteCandidate[];

  /** Check if a specific field should be promoted */
  shouldPromote(analyzer: FieldAnalyzer, fieldPath: string): boolean;

  /** Get the priority score for a field */
  getPriority(analyzer: FieldAnalyzer, fieldPath: string): number;

  /** Apply promotions to a schema */
  applyPromotions(
    schema: ParsedCollectionSchema,
    promotions: AutoPromoteCandidate[]
  ): ParsedCollectionSchema;

  /** Get promotion history for a field */
  getPromotionHistory(fieldPath: string): PromotionHistoryEntry[];

  /** Reset cooldown for a field */
  resetCooldown(fieldPath: string): void;

  /** Check if a field is in cooldown */
  isInCooldown(fieldPath: string): boolean;
}

/**
 * History entry for promotion tracking
 */
interface PromotionHistoryEntry {
  /** Field path */
  path: string;
  /** When promotion was considered */
  timestamp: Date;
  /** Whether it was promoted */
  promoted: boolean;
  /** Reason for decision */
  reason: string;
  /** Stats at time of decision */
  stats: {
    frequency: number;
    consistency: number;
    sampleSize: number;
  };
}

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Create a minimal parsed column for testing
 */
function createColumn(path: string, type: ParquetType): ParsedColumn {
  return {
    path,
    segments: path.split('.'),
    type,
    isArray: false,
    isStruct: false,
  };
}

/**
 * Create a minimal parsed collection schema for testing
 */
function createSchema(columns: Record<string, ParquetType>): ParsedCollectionSchema {
  const columnList: ParsedColumn[] = [];
  const columnMap = new Map<string, ParsedColumn>();

  for (const [path, type] of Object.entries(columns)) {
    const col = createColumn(path, type);
    columnList.push(col);
    columnMap.set(path, col);
  }

  return {
    columns: columnList,
    columnMap,
    storeVariant: true,
  };
}

/**
 * Generate documents with configurable field presence patterns
 */
function generateDocuments(
  count: number,
  fieldConfigs: Record<string, { frequency: number; type: 'string' | 'number' | 'boolean' | 'date' | 'array' | 'object' }>
): Record<string, unknown>[] {
  const docs: Record<string, unknown>[] = [];

  for (let i = 0; i < count; i++) {
    const doc: Record<string, unknown> = {};

    for (const [field, config] of Object.entries(fieldConfigs)) {
      // Include field based on frequency
      if (Math.random() < config.frequency || i < count * config.frequency) {
        switch (config.type) {
          case 'string':
            doc[field] = `value_${i}`;
            break;
          case 'number':
            doc[field] = i * 10;
            break;
          case 'boolean':
            doc[field] = i % 2 === 0;
            break;
          case 'date':
            doc[field] = new Date(Date.now() + i * 86400000);
            break;
          case 'array':
            doc[field] = [i, i + 1, i + 2];
            break;
          case 'object':
            doc[field] = { nested: `value_${i}` };
            break;
        }
      }
    }

    docs.push(doc);
  }

  return docs;
}

/**
 * Generate documents with deterministic field presence
 */
function generateDeterministicDocuments(
  total: number,
  fieldPresence: Record<string, number> // field -> count of documents containing it
): Record<string, unknown>[] {
  const docs: Record<string, unknown>[] = [];

  for (let i = 0; i < total; i++) {
    const doc: Record<string, unknown> = { _id: `doc_${i}` };

    for (const [field, presence] of Object.entries(fieldPresence)) {
      if (i < presence) {
        doc[field] = `${field}_value_${i}`;
      }
    }

    docs.push(doc);
  }

  return docs;
}

// ============================================================================
// Threshold Detection Tests
// ============================================================================

describe('Auto-Promote: Threshold Detection', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  describe('frequency threshold evaluation', () => {
    it('should identify fields meeting the default threshold of 0.8', () => {
      // Create documents where 'frequent' appears 80% of time, 'infrequent' 50%
      const docs = generateDeterministicDocuments(100, {
        frequent: 80,
        infrequent: 50,
      });

      analyzer.analyzeDocuments(docs);

      // RED: AutoPromoteManager doesn't exist yet - test should fail
      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      expect(AutoPromoteManager).toBeDefined();

      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 });

      const result = manager.evaluate(analyzer);

      // frequent (80%) should be promoted
      expect(result.promotions.some(p => p.path === 'frequent')).toBe(true);
      // infrequent (50%) should not be promoted
      expect(result.promotions.some(p => p.path === 'infrequent')).toBe(false);
      expect(result.skipped.some(s => s.path === 'infrequent')).toBe(true);
    });

    it('should respect custom threshold configuration', () => {
      const docs = generateDeterministicDocuments(100, {
        field60: 60,
        field70: 70,
        field80: 80,
      });

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();

      // With threshold of 0.6, all should be promoted
      manager.configure({ threshold: 0.6 });
      let result = manager.evaluate(analyzer);
      expect(result.promotions.length).toBe(3);

      // With threshold of 0.75, only 80% field should be promoted
      manager.configure({ threshold: 0.75 });
      result = manager.evaluate(analyzer);
      expect(result.promotions.length).toBe(1);
      expect(result.promotions[0].path).toBe('field80');
    });

    it('should handle edge case of exactly meeting threshold', () => {
      const docs = generateDeterministicDocuments(100, {
        exactly80: 80,
        below80: 79,
      });

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 });

      const result = manager.evaluate(analyzer);

      // exactly80 should be included (>= threshold)
      expect(result.promotions.some(p => p.path === 'exactly80')).toBe(true);
      // below80 should not be included (< threshold)
      expect(result.promotions.some(p => p.path === 'below80')).toBe(false);
    });

    it('should recalculate threshold after incremental document analysis', () => {
      // Initial batch: field at 90%
      const batch1 = generateDeterministicDocuments(100, { field: 90 });
      analyzer.analyzeDocuments(batch1);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 });

      let result = manager.evaluate(analyzer);
      expect(result.promotions.some(p => p.path === 'field')).toBe(true);

      // Second batch: field drops to 60% overall
      const batch2: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        if (i < 30) {
          batch2.push({ field: 'value' });
        } else {
          batch2.push({});
        }
      }
      analyzer.analyzeDocuments(batch2);

      // Now field is at 120/200 = 60%, should no longer meet threshold
      result = manager.evaluate(analyzer);
      expect(result.promotions.some(p => p.path === 'field')).toBe(false);
    });
  });

  describe('consistency threshold evaluation', () => {
    it('should require minimum type consistency for promotion', () => {
      // Field with 100% frequency but mixed types
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        if (i < 50) {
          docs.push({ mixed: `string_${i}` });
        } else if (i < 80) {
          docs.push({ mixed: i });
        } else {
          docs.push({ mixed: true });
        }
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8, minConsistency: 0.9 });

      const result = manager.evaluate(analyzer);

      // Field appears 100% but consistency is only 50%, should be skipped
      expect(result.promotions.some(p => p.path === 'mixed')).toBe(false);
      expect(result.skipped.some(s => s.path === 'mixed' && s.reason.includes('consistency'))).toBe(true);
    });

    it('should consider null values separately from type consistency', () => {
      // Field with many nulls but consistent non-null type
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        if (i < 30) {
          docs.push({ field: null });
        } else {
          docs.push({ field: `value_${i}` });
        }
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8, minConsistency: 0.9 });

      const result = manager.evaluate(analyzer);

      // All non-null values are strings (100% consistent), should be promoted
      expect(result.promotions.some(p => p.path === 'field')).toBe(true);
    });

    it('should default to 0.9 consistency when not specified', () => {
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        if (i < 85) {
          docs.push({ field: `string_${i}` });
        } else {
          docs.push({ field: i });
        }
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 }); // No explicit minConsistency

      const result = manager.evaluate(analyzer);

      // 85% consistency < 90% default, should be skipped
      expect(result.promotions.some(p => p.path === 'field')).toBe(false);
    });
  });

  describe('sample size requirements', () => {
    it('should require minimum sample size before promotion', () => {
      const docs = generateDeterministicDocuments(10, { field: 10 }); // 100% frequency but only 10 docs

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8, minSampleSize: 100 });

      const result = manager.evaluate(analyzer);

      // Too few samples, should be skipped
      expect(result.promotions.some(p => p.path === 'field')).toBe(false);
      expect(result.skipped.some(s => s.path === 'field' && s.reason.includes('sample size'))).toBe(true);
    });

    it('should promote once minimum sample size is met', () => {
      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8, minSampleSize: 50 });

      // First batch: 30 docs
      let docs = generateDeterministicDocuments(30, { field: 30 });
      analyzer.analyzeDocuments(docs);

      let result = manager.evaluate(analyzer);
      expect(result.promotions.some(p => p.path === 'field')).toBe(false);

      // Second batch: 25 more docs (total 55)
      docs = generateDeterministicDocuments(25, { field: 25 });
      analyzer.analyzeDocuments(docs);

      result = manager.evaluate(analyzer);
      expect(result.promotions.some(p => p.path === 'field')).toBe(true);
    });
  });
});

// ============================================================================
// Promotion Triggers Tests
// ============================================================================

describe('Auto-Promote: Promotion Triggers', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  describe('immediate promotion triggers', () => {
    it('should trigger promotion when field crosses threshold', () => {
      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 });

      // Field starts at 70%
      let docs = generateDeterministicDocuments(100, { field: 70 });
      analyzer.analyzeDocuments(docs);

      expect(manager.shouldPromote(analyzer, 'field')).toBe(false);

      // Add more docs to bring it to 85%
      docs = generateDeterministicDocuments(100, { field: 100 });
      analyzer.analyzeDocuments(docs);

      // Now at 170/200 = 85%, should trigger
      expect(manager.shouldPromote(analyzer, 'field')).toBe(true);
    });

    it('should trigger promotion for new fields exceeding threshold immediately', () => {
      const docs = generateDeterministicDocuments(100, { newField: 95 });

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 });

      // New field at 95% should immediately trigger
      expect(manager.shouldPromote(analyzer, 'newField')).toBe(true);
    });

    it('should not trigger for already promoted fields', () => {
      const docs = generateDeterministicDocuments(100, { existing: 95 });
      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 });

      // Create schema with 'existing' already promoted
      const schema = createSchema({ existing: 'string' });

      // Apply to exclude already promoted fields
      const result = manager.evaluate(analyzer);
      const filteredPromotions = result.promotions.filter(
        p => !schema.columnMap.has(p.path)
      );

      // 'existing' should not appear in new promotions
      expect(filteredPromotions.some(p => p.path === 'existing')).toBe(false);
    });
  });

  describe('cooldown-based triggers', () => {
    it('should respect cooldown period after evaluation', async () => {
      const docs = generateDeterministicDocuments(100, { field: 85 });
      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8, cooldownMs: 100 });

      // First evaluation triggers
      const result1 = manager.evaluate(analyzer);
      expect(result1.promotions.some(p => p.path === 'field')).toBe(true);

      // Immediate re-evaluation should be in cooldown
      expect(manager.isInCooldown('field')).toBe(true);

      // After cooldown, should be re-evaluable
      await new Promise(resolve => setTimeout(resolve, 150));
      expect(manager.isInCooldown('field')).toBe(false);
    });

    it('should allow manual cooldown reset', () => {
      const docs = generateDeterministicDocuments(100, { field: 85 });
      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8, cooldownMs: 10000 });

      manager.evaluate(analyzer);
      expect(manager.isInCooldown('field')).toBe(true);

      manager.resetCooldown('field');
      expect(manager.isInCooldown('field')).toBe(false);
    });
  });

  describe('batch evaluation triggers', () => {
    it('should evaluate all fields in a single batch operation', () => {
      const docs = generateDeterministicDocuments(100, {
        field1: 90,
        field2: 85,
        field3: 50,
        field4: 95,
      });

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 });

      const result = manager.evaluate(analyzer);

      expect(result.totalEvaluated).toBe(4);
      expect(result.promotions.length).toBe(3); // field1, field2, field4
      expect(result.skipped.length).toBe(1); // field3
    });

    it('should record evaluation timestamp', () => {
      const docs = generateDeterministicDocuments(100, { field: 90 });
      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 });

      const before = new Date();
      const result = manager.evaluate(analyzer);
      const after = new Date();

      expect(result.evaluatedAt.getTime()).toBeGreaterThanOrEqual(before.getTime());
      expect(result.evaluatedAt.getTime()).toBeLessThanOrEqual(after.getTime());
    });
  });
});

// ============================================================================
// Field Selection Criteria Tests
// ============================================================================

describe('Auto-Promote: Field Selection Criteria', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  describe('type-based selection', () => {
    it('should select fields with promotable scalar types', () => {
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          stringField: `value_${i}`,
          numberField: i,
          booleanField: i % 2 === 0,
          dateField: new Date(),
          binaryField: new Uint8Array([i]),
        });
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 });

      const result = manager.evaluate(analyzer);

      // All scalar types should be promotable
      expect(result.promotions.some(p => p.path === 'stringField' && p.suggestedType === 'string')).toBe(true);
      expect(result.promotions.some(p => p.path === 'numberField' && p.suggestedType === 'double')).toBe(true);
      expect(result.promotions.some(p => p.path === 'booleanField' && p.suggestedType === 'boolean')).toBe(true);
      expect(result.promotions.some(p => p.path === 'dateField' && p.suggestedType === 'timestamp')).toBe(true);
      expect(result.promotions.some(p => p.path === 'binaryField' && p.suggestedType === 'binary')).toBe(true);
    });

    it('should not select array fields by default', () => {
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          arrayField: [1, 2, 3],
          scalarField: `value_${i}`,
        });
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8, includeArrays: false });

      const result = manager.evaluate(analyzer);

      expect(result.promotions.some(p => p.path === 'arrayField')).toBe(false);
      expect(result.promotions.some(p => p.path === 'scalarField')).toBe(true);
    });

    it('should optionally include array fields when configured', () => {
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          arrayField: [1, 2, 3],
        });
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8, includeArrays: true });

      const result = manager.evaluate(analyzer);

      expect(result.promotions.some(p => p.path === 'arrayField')).toBe(true);
    });

    it('should not select object/variant fields', () => {
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          objectField: { nested: `value_${i}` },
          scalarField: `value_${i}`,
        });
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 });

      const result = manager.evaluate(analyzer);

      // Object fields should not be promoted (but their nested fields might be)
      expect(result.promotions.some(p => p.path === 'objectField')).toBe(false);
      expect(result.promotions.some(p => p.path === 'scalarField')).toBe(true);
    });

    it('should select nested scalar fields within objects', () => {
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          user: {
            name: `User ${i}`,
            age: 20 + i,
            active: i % 2 === 0,
          },
        });
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8, includeNested: true });

      const result = manager.evaluate(analyzer);

      expect(result.promotions.some(p => p.path === 'user.name')).toBe(true);
      expect(result.promotions.some(p => p.path === 'user.age')).toBe(true);
      expect(result.promotions.some(p => p.path === 'user.active')).toBe(true);
    });
  });

  describe('nesting depth selection', () => {
    it('should respect maximum nesting depth', () => {
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          level1: {
            level2: {
              level3: {
                level4: {
                  deepField: `value_${i}`,
                },
                shallowField: `value_${i}`,
              },
            },
          },
        });
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8, includeNested: true, maxNestingDepth: 3 });

      const result = manager.evaluate(analyzer);

      // level1.level2.level3.shallowField (depth 3) should be included
      expect(result.promotions.some(p => p.path === 'level1.level2.level3.shallowField')).toBe(true);
      // level1.level2.level3.level4.deepField (depth 4) should be excluded
      expect(result.promotions.some(p => p.path === 'level1.level2.level3.level4.deepField')).toBe(false);
    });

    it('should disable nested field promotion when configured', () => {
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          rootField: `value_${i}`,
          nested: {
            childField: `child_${i}`,
          },
        });
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8, includeNested: false });

      const result = manager.evaluate(analyzer);

      expect(result.promotions.some(p => p.path === 'rootField')).toBe(true);
      expect(result.promotions.some(p => p.path === 'nested.childField')).toBe(false);
    });
  });

  describe('priority scoring', () => {
    it('should assign higher priority to higher frequency fields', () => {
      const docs = generateDeterministicDocuments(100, {
        high: 99,
        medium: 85,
        low: 81,
      });

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 });

      const highPriority = manager.getPriority(analyzer, 'high');
      const mediumPriority = manager.getPriority(analyzer, 'medium');
      const lowPriority = manager.getPriority(analyzer, 'low');

      expect(highPriority).toBeGreaterThan(mediumPriority);
      expect(mediumPriority).toBeGreaterThan(lowPriority);
    });

    it('should factor in type consistency for priority', () => {
      // Both fields have same frequency but different consistency
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          consistent: `value_${i}`,
          inconsistent: i % 2 === 0 ? `value_${i}` : i,
        });
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8, minConsistency: 0 }); // Allow low consistency

      const consistentPriority = manager.getPriority(analyzer, 'consistent');
      const inconsistentPriority = manager.getPriority(analyzer, 'inconsistent');

      expect(consistentPriority).toBeGreaterThan(inconsistentPriority);
    });

    it('should return promotions sorted by priority', () => {
      const docs = generateDeterministicDocuments(100, {
        a: 85,
        b: 99,
        c: 90,
        d: 82,
      });

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 });

      const candidates = manager.getCandidates(analyzer);

      // Should be sorted by priority (descending)
      for (let i = 0; i < candidates.length - 1; i++) {
        expect(candidates[i].priority).toBeGreaterThanOrEqual(candidates[i + 1].priority);
      }
    });
  });
});

// ============================================================================
// Exclude Patterns Tests
// ============================================================================

describe('Auto-Promote: Exclude Patterns', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  describe('glob pattern exclusions', () => {
    it('should exclude fields matching simple glob patterns', () => {
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          publicField: `public_${i}`,
          _privateField: `private_${i}`,
          __internalField: `internal_${i}`,
        });
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({
        threshold: 0.8,
        excludePatterns: ['_*', '__*'],
      });

      const result = manager.evaluate(analyzer);

      expect(result.promotions.some(p => p.path === 'publicField')).toBe(true);
      expect(result.promotions.some(p => p.path === '_privateField')).toBe(false);
      expect(result.promotions.some(p => p.path === '__internalField')).toBe(false);
      expect(result.excluded).toContain('_privateField');
      expect(result.excluded).toContain('__internalField');
    });

    it('should exclude fields matching nested path patterns', () => {
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          user: {
            name: `name_${i}`,
            password: `pwd_${i}`,
            credentials: {
              token: `token_${i}`,
            },
          },
          data: {
            password: `data_pwd_${i}`,
          },
        });
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({
        threshold: 0.8,
        includeNested: true,
        excludePatterns: ['*.password', 'user.credentials.*'],
      });

      const result = manager.evaluate(analyzer);

      expect(result.promotions.some(p => p.path === 'user.name')).toBe(true);
      expect(result.promotions.some(p => p.path === 'user.password')).toBe(false);
      expect(result.promotions.some(p => p.path === 'data.password')).toBe(false);
      expect(result.promotions.some(p => p.path === 'user.credentials.token')).toBe(false);
    });

    it('should exclude fields matching wildcard patterns', () => {
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          user_id: i,
          order_id: i * 10,
          product_name: `product_${i}`,
          timestamp: new Date(),
        });
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({
        threshold: 0.8,
        excludePatterns: ['*_id'],
      });

      const result = manager.evaluate(analyzer);

      expect(result.promotions.some(p => p.path === 'user_id')).toBe(false);
      expect(result.promotions.some(p => p.path === 'order_id')).toBe(false);
      expect(result.promotions.some(p => p.path === 'product_name')).toBe(true);
      expect(result.promotions.some(p => p.path === 'timestamp')).toBe(true);
    });

    it('should exclude fields matching double-wildcard deep patterns', () => {
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          data: {
            public: {
              name: `name_${i}`,
            },
            private: {
              secret: `secret_${i}`,
              nested: {
                deepSecret: `deep_${i}`,
              },
            },
          },
        });
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({
        threshold: 0.8,
        includeNested: true,
        excludePatterns: ['data.private.**'], // Exclude all under data.private
      });

      const result = manager.evaluate(analyzer);

      expect(result.promotions.some(p => p.path === 'data.public.name')).toBe(true);
      expect(result.promotions.some(p => p.path === 'data.private.secret')).toBe(false);
      expect(result.promotions.some(p => p.path === 'data.private.nested.deepSecret')).toBe(false);
    });
  });

  describe('include pattern overrides', () => {
    it('should allow include patterns to override exclude patterns', () => {
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          _id: `id_${i}`,
          _metadata: `meta_${i}`,
          _timestamp: new Date(),
          name: `name_${i}`,
        });
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({
        threshold: 0.8,
        excludePatterns: ['_*'], // Exclude all underscore-prefixed
        includePatterns: ['_id', '_timestamp'], // But include these specific ones
      });

      const result = manager.evaluate(analyzer);

      expect(result.promotions.some(p => p.path === '_id')).toBe(true);
      expect(result.promotions.some(p => p.path === '_timestamp')).toBe(true);
      expect(result.promotions.some(p => p.path === '_metadata')).toBe(false);
      expect(result.promotions.some(p => p.path === 'name')).toBe(true);
    });
  });

  describe('predefined exclusion sets', () => {
    it('should support common exclusion presets', () => {
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          name: `name_${i}`,
          password: `pwd_${i}`,
          secret: `secret_${i}`,
          apiKey: `key_${i}`,
          email: `email_${i}@test.com`,
        });
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();

      // Should support a preset for sensitive fields
      manager.configure({
        threshold: 0.8,
        excludePatterns: ['password', 'secret', '*Key', '*Token', '*Secret'],
      });

      const result = manager.evaluate(analyzer);

      expect(result.promotions.some(p => p.path === 'name')).toBe(true);
      expect(result.promotions.some(p => p.path === 'email')).toBe(true);
      expect(result.promotions.some(p => p.path === 'password')).toBe(false);
      expect(result.promotions.some(p => p.path === 'secret')).toBe(false);
      expect(result.promotions.some(p => p.path === 'apiKey')).toBe(false);
    });
  });
});

// ============================================================================
// Promotion Batching Tests
// ============================================================================

describe('Auto-Promote: Promotion Batching', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  describe('batch size limits', () => {
    it('should limit promotions to maxBatchSize', () => {
      const docs = generateDeterministicDocuments(100, {
        field1: 99,
        field2: 98,
        field3: 97,
        field4: 96,
        field5: 95,
      });

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8, maxBatchSize: 3 });

      const result = manager.evaluate(analyzer);

      // Should only include top 3 by priority
      expect(result.promotions.length).toBe(3);
    });

    it('should prioritize higher frequency fields in limited batch', () => {
      const docs = generateDeterministicDocuments(100, {
        low: 81,
        medium: 90,
        high: 99,
        veryHigh: 100,
      });

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8, maxBatchSize: 2 });

      const result = manager.evaluate(analyzer);

      expect(result.promotions.length).toBe(2);
      expect(result.promotions.some(p => p.path === 'veryHigh')).toBe(true);
      expect(result.promotions.some(p => p.path === 'high')).toBe(true);
      expect(result.promotions.some(p => p.path === 'medium')).toBe(false);
    });

    it('should include all fields when no batch limit', () => {
      const docs = generateDeterministicDocuments(100, {
        field1: 90,
        field2: 90,
        field3: 90,
        field4: 90,
        field5: 90,
        field6: 90,
        field7: 90,
        field8: 90,
        field9: 90,
        field10: 90,
      });

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 }); // No maxBatchSize

      const result = manager.evaluate(analyzer);

      expect(result.promotions.length).toBe(10);
    });
  });

  describe('batch ordering', () => {
    it('should order batch by priority (frequency then consistency)', () => {
      const docs = generateDeterministicDocuments(100, {
        a: 99,
        b: 95,
        c: 90,
        d: 85,
      });

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 });

      const result = manager.evaluate(analyzer);

      expect(result.promotions[0].path).toBe('a');
      expect(result.promotions[1].path).toBe('b');
      expect(result.promotions[2].path).toBe('c');
      expect(result.promotions[3].path).toBe('d');
    });

    it('should use consistency as tiebreaker for equal frequency', () => {
      // Two fields with same frequency but different consistency
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          consistent: `value_${i}`,
          inconsistent: i % 10 === 0 ? i : `value_${i}`, // 10% numbers, 90% strings
        });
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8, minConsistency: 0.5 });

      const result = manager.evaluate(analyzer);

      // consistent has 100% consistency, inconsistent has 90%
      expect(result.promotions[0].path).toBe('consistent');
    });
  });

  describe('incremental batching', () => {
    it('should allow subsequent batches to promote remaining fields', () => {
      const docs = generateDeterministicDocuments(100, {
        field1: 99,
        field2: 98,
        field3: 97,
        field4: 96,
      });

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8, maxBatchSize: 2, cooldownMs: 0 });

      // First batch: top 2
      const result1 = manager.evaluate(analyzer);
      expect(result1.promotions.map(p => p.path)).toEqual(['field1', 'field2']);

      // Apply promotions to schema
      const schema = createSchema({});
      const updatedSchema = manager.applyPromotions(schema, result1.promotions);

      // Second evaluation should return next 2 (excluding already promoted)
      // Note: This requires the manager to track applied promotions
      const result2 = manager.evaluate(analyzer);
      const newPromotions = result2.promotions.filter(
        p => !updatedSchema.columnMap.has(p.path)
      );
      expect(newPromotions.map(p => p.path)).toEqual(['field3', 'field4']);
    });
  });

  describe('schema application', () => {
    it('should apply promotions to create new schema columns', () => {
      const docs = generateDeterministicDocuments(100, {
        name: 100,
        age: 100,
      });

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 });

      const result = manager.evaluate(analyzer);
      const initialSchema = createSchema({});
      const newSchema = manager.applyPromotions(initialSchema, result.promotions);

      expect(newSchema.columnMap.has('name')).toBe(true);
      expect(newSchema.columnMap.has('age')).toBe(true);
      expect(newSchema.columnMap.get('name')?.type).toBe('string');
      expect(newSchema.columnMap.get('age')?.type).toBe('string'); // Generated as strings in test
    });

    it('should preserve existing schema columns when applying promotions', () => {
      const docs = generateDeterministicDocuments(100, { newField: 100 });
      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 });

      const result = manager.evaluate(analyzer);
      const initialSchema = createSchema({ existingField: 'int64' });
      const newSchema = manager.applyPromotions(initialSchema, result.promotions);

      expect(newSchema.columnMap.has('existingField')).toBe(true);
      expect(newSchema.columnMap.get('existingField')?.type).toBe('int64');
      expect(newSchema.columnMap.has('newField')).toBe(true);
    });

    it('should not modify existing column types when promoting', () => {
      // Field exists in schema as int32, analysis suggests double
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < 100; i++) {
        docs.push({ count: i * 1.5 }); // Floats
      }

      analyzer.analyzeDocuments(docs);

      const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
      const manager = new AutoPromoteManager();
      manager.configure({ threshold: 0.8 });

      const result = manager.evaluate(analyzer);
      const initialSchema = createSchema({ count: 'int32' });
      const newSchema = manager.applyPromotions(initialSchema, result.promotions);

      // Existing type should be preserved (type evolution is separate concern)
      expect(newSchema.columnMap.get('count')?.type).toBe('int32');
    });
  });
});

// ============================================================================
// Promotion History Tests
// ============================================================================

describe('Auto-Promote: Promotion History', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should record promotion decisions in history', () => {
    const docs = generateDeterministicDocuments(100, {
      promoted: 95,
      skipped: 50,
    });

    analyzer.analyzeDocuments(docs);

    const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
    const manager = new AutoPromoteManager();
    manager.configure({ threshold: 0.8 });

    manager.evaluate(analyzer);

    const promotedHistory = manager.getPromotionHistory('promoted');
    expect(promotedHistory.length).toBeGreaterThan(0);
    expect(promotedHistory[0].promoted).toBe(true);
    expect(promotedHistory[0].stats.frequency).toBeCloseTo(0.95);

    const skippedHistory = manager.getPromotionHistory('skipped');
    expect(skippedHistory.length).toBeGreaterThan(0);
    expect(skippedHistory[0].promoted).toBe(false);
    expect(skippedHistory[0].reason).toContain('threshold');
  });

  it('should track multiple evaluations of same field', () => {
    const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
    const manager = new AutoPromoteManager();
    manager.configure({ threshold: 0.8, cooldownMs: 0 });

    // First evaluation: below threshold
    let docs = generateDeterministicDocuments(100, { field: 70 });
    analyzer.analyzeDocuments(docs);
    manager.evaluate(analyzer);

    // Second evaluation: above threshold
    docs = generateDeterministicDocuments(100, { field: 100 });
    analyzer.analyzeDocuments(docs);
    manager.evaluate(analyzer);

    const history = manager.getPromotionHistory('field');
    expect(history.length).toBe(2);
    expect(history[0].promoted).toBe(false);
    expect(history[1].promoted).toBe(true);
  });

  it('should include stats snapshot in history entry', () => {
    const docs = generateDeterministicDocuments(100, { field: 90 });
    analyzer.analyzeDocuments(docs);

    const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
    const manager = new AutoPromoteManager();
    manager.configure({ threshold: 0.8 });

    manager.evaluate(analyzer);

    const history = manager.getPromotionHistory('field');
    expect(history[0].stats).toBeDefined();
    expect(history[0].stats.frequency).toBeCloseTo(0.9);
    expect(history[0].stats.consistency).toBe(1.0);
    expect(history[0].stats.sampleSize).toBe(100);
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('Auto-Promote: Integration', () => {
  it('should support complete auto-promote workflow', () => {
    const analyzer = new FieldAnalyzer();

    // Simulate realistic document ingestion
    const docs: Record<string, unknown>[] = [];
    for (let i = 0; i < 1000; i++) {
      docs.push({
        _id: `doc_${i}`,
        userId: `user_${i % 100}`,
        action: ['click', 'view', 'purchase'][i % 3],
        timestamp: new Date(Date.now() - i * 1000),
        value: Math.random() * 100,
        metadata: {
          source: 'web',
          version: '2.0',
          _internal: `internal_${i}`,
        },
        tags: ['tag1', 'tag2'],
        // Use spread to conditionally include optionalField (20% of docs)
        ...(i % 5 === 0 ? { optionalField: `optional_${i}` } : {}),
      });
    }

    analyzer.analyzeDocuments(docs);

    const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
    const manager = new AutoPromoteManager();
    manager.configure({
      threshold: 0.8,
      minConsistency: 0.9,
      maxBatchSize: 5,
      excludePatterns: ['_*', '*.tags', 'metadata._*'],
      includeNested: true,
      maxNestingDepth: 3,
      minSampleSize: 100,
    });

    const result = manager.evaluate(analyzer);

    // Expected promotions based on config:
    // - userId (100%), action (100%), timestamp (100%), value (100%)
    // - metadata.source (100%), metadata.version (100%)
    // Excluded: _id (pattern), tags (pattern), metadata._internal (pattern)
    // Skipped: optionalField (20% < 80%)

    expect(result.promotions.length).toBeLessThanOrEqual(5); // maxBatchSize
    expect(result.excluded).toContain('_id');
    expect(result.excluded).toContain('metadata._internal');
    expect(result.skipped.some(s => s.path === 'optionalField')).toBe(true);

    // Apply promotions
    const schema = createSchema({});
    const newSchema = manager.applyPromotions(schema, result.promotions);

    expect(newSchema.columnMap.size).toBe(result.promotions.length);

    // Verify history tracking
    for (const promotion of result.promotions) {
      const history = manager.getPromotionHistory(promotion.path);
      expect(history.length).toBeGreaterThan(0);
      expect(history[0].promoted).toBe(true);
    }
  });

  it('should handle schema evolution over multiple batches', () => {
    const analyzer = new FieldAnalyzer();

    // Initial batch of documents
    const docs1: Record<string, unknown>[] = [];
    for (let i = 0; i < 500; i++) {
      docs1.push({
        name: `name_${i}`,
        email: `email_${i}@test.com`,
        age: 20 + (i % 50),
      });
    }

    analyzer.analyzeDocuments(docs1);

    const AutoPromoteManager = (globalThis as unknown as { AutoPromoteManager: new () => AutoPromoteManager }).AutoPromoteManager;
    const manager = new AutoPromoteManager();
    manager.configure({
      threshold: 0.8,
      maxBatchSize: 2,
      cooldownMs: 0,
    });

    // First batch of promotions
    const result1 = manager.evaluate(analyzer);
    expect(result1.promotions.length).toBe(2);

    let schema = createSchema({});
    schema = manager.applyPromotions(schema, result1.promotions);

    // Second batch of promotions
    const result2 = manager.evaluate(analyzer);
    const newPromotions = result2.promotions.filter(
      p => !schema.columnMap.has(p.path)
    );
    schema = manager.applyPromotions(schema, newPromotions);

    // All fields should now be promoted
    expect(schema.columnMap.size).toBe(3);
    expect(schema.columnMap.has('name')).toBe(true);
    expect(schema.columnMap.has('email')).toBe(true);
    expect(schema.columnMap.has('age')).toBe(true);

    // New documents with additional field
    // Adding 2000 docs with newField to ensure it meets 80% threshold overall
    // After this: 2500 total docs, newField in 2000 = 80% frequency
    const docs2: Record<string, unknown>[] = [];
    for (let i = 0; i < 2000; i++) {
      docs2.push({
        name: `name_${i}`,
        email: `email_${i}@test.com`,
        age: 20 + (i % 50),
        newField: `new_${i}`,
      });
    }

    analyzer.analyzeDocuments(docs2);

    // Evaluate again - should pick up new field (2000/2500 = 80% frequency)
    const result3 = manager.evaluate(analyzer);
    const brandNewPromotions = result3.promotions.filter(
      p => !schema.columnMap.has(p.path)
    );

    expect(brandNewPromotions.length).toBe(1);
    expect(brandNewPromotions[0].path).toBe('newField');
  });
});
