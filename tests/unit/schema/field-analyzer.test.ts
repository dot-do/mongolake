/**
 * RED Phase Tests: Field Analyzer
 *
 * These tests are designed to FAIL initially, validating the expected behavior
 * for the field analyzer feature. They cover:
 * - Detect field presence
 * - Track type distribution
 * - Identify nested patterns
 * - Calculate occurrence frequency
 * - Handle null/missing values
 *
 * @module tests/unit/schema/field-analyzer
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  FieldAnalyzer,
  createFieldAnalyzer,
  analyzeDocuments,
  suggestFieldPromotions,
  type FieldStats,
  type PromotionSuggestion,
  type DetectedType,
} from '../../../src/schema/analyzer.js';
import { ObjectId } from '../../../src/types.js';

// ============================================================================
// Test Fixtures
// ============================================================================

/**
 * Create documents with various field presence patterns
 */
function createFieldPresenceDocuments(): Record<string, unknown>[] {
  return [
    { always: 'a', sometimes: 'x', rarely: 'r' },
    { always: 'b', sometimes: 'y' },
    { always: 'c', sometimes: 'z' },
    { always: 'd' },
    { always: 'e' },
    { always: 'f' },
    { always: 'g' },
    { always: 'h' },
    { always: 'i' },
    { always: 'j' },
  ];
}

/**
 * Create documents with diverse type distributions
 */
function createTypeDistributionDocuments(): Record<string, unknown>[] {
  return [
    { mixed: 'string1' },
    { mixed: 'string2' },
    { mixed: 'string3' },
    { mixed: 42 },
    { mixed: 43 },
    { mixed: true },
    { mixed: null },
    { mixed: new Date() },
    { mixed: ['array'] },
    { mixed: { nested: 'object' } },
  ];
}

/**
 * Create documents with complex nested structures
 */
function createComplexNestedDocuments(): Record<string, unknown>[] {
  return [
    {
      metadata: {
        source: {
          type: 'api',
          version: '1.0',
          details: {
            endpoint: '/users',
            method: 'POST',
          },
        },
        timestamps: {
          created: new Date('2024-01-01'),
          modified: new Date('2024-01-15'),
        },
      },
      data: {
        users: [
          { id: 1, name: 'Alice' },
          { id: 2, name: 'Bob' },
        ],
      },
    },
    {
      metadata: {
        source: {
          type: 'batch',
          version: '2.0',
        },
        timestamps: {
          created: new Date('2024-02-01'),
        },
      },
      data: {
        users: [
          { id: 3, name: 'Charlie' },
        ],
      },
    },
    {
      metadata: {
        source: {
          type: 'stream',
        },
      },
      data: {
        events: [
          { type: 'click', count: 5 },
        ],
      },
    },
  ];
}

/**
 * Create documents with null and missing value patterns
 */
function createNullMissingDocuments(): Record<string, unknown>[] {
  return [
    { field: 'value1', optional: 'present', nullable: null },
    { field: 'value2', nullable: 'not-null' },
    { field: 'value3', optional: null, nullable: null },
    { field: undefined, optional: 'present' },
    { field: 'value5' },
  ];
}

// ============================================================================
// Field Presence Detection Tests
// ============================================================================

describe('FieldAnalyzer field presence detection', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should detect fields that appear in all documents', () => {
    analyzer.analyzeDocuments(createFieldPresenceDocuments());

    const stats = analyzer.getFieldStat('always');
    expect(stats).toBeDefined();
    expect(stats?.count).toBe(10);
    expect(stats?.frequency).toBe(1.0);
  });

  it('should detect fields that appear in some documents', () => {
    analyzer.analyzeDocuments(createFieldPresenceDocuments());

    const stats = analyzer.getFieldStat('sometimes');
    expect(stats).toBeDefined();
    expect(stats?.count).toBe(3);
    expect(stats?.frequency).toBe(0.3);
  });

  it('should detect fields that rarely appear', () => {
    analyzer.analyzeDocuments(createFieldPresenceDocuments());

    const stats = analyzer.getFieldStat('rarely');
    expect(stats).toBeDefined();
    expect(stats?.count).toBe(1);
    expect(stats?.frequency).toBe(0.1);
  });

  it('should return undefined for fields that never appear', () => {
    analyzer.analyzeDocuments(createFieldPresenceDocuments());

    const stats = analyzer.getFieldStat('nonexistent');
    expect(stats).toBeUndefined();
  });

  it('should track field presence separately from null values', () => {
    analyzer.analyzeDocuments([
      { field: 'value' },
      { field: null },
      { field: 'another' },
      {}, // field is missing
    ]);

    const stats = analyzer.getFieldStat('field');
    expect(stats).toBeDefined();
    // Field is present in 3 documents (including null), missing in 1
    expect(stats?.count).toBe(3);
    expect(stats?.frequency).toBe(0.75);
  });

  it('should distinguish between undefined and missing fields', () => {
    analyzer.analyzeDocuments([
      { a: 1, b: undefined },
      { a: 2 }, // b is missing
      { a: 3, b: 'value' },
    ]);

    const bStats = analyzer.getFieldStat('b');
    // b is present in documents 1 and 3 (undefined counts as present)
    expect(bStats?.count).toBe(2);
    expect(bStats?.types.get('null')).toBe(1); // undefined becomes null
    expect(bStats?.types.get('string')).toBe(1);
  });
});

// ============================================================================
// Type Distribution Tracking Tests
// ============================================================================

describe('FieldAnalyzer type distribution tracking', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should track exact distribution of types for a field', () => {
    analyzer.analyzeDocuments(createTypeDistributionDocuments());

    const stats = analyzer.getFieldStat('mixed');
    expect(stats).toBeDefined();
    expect(stats?.types.get('string')).toBe(3);
    expect(stats?.types.get('number')).toBe(2);
    expect(stats?.types.get('boolean')).toBe(1);
    expect(stats?.types.get('null')).toBe(1);
    expect(stats?.types.get('date')).toBe(1);
    expect(stats?.types.get('array')).toBe(1);
    expect(stats?.types.get('object')).toBe(1);
  });

  it('should calculate type distribution percentages', () => {
    analyzer.analyzeDocuments(createTypeDistributionDocuments());

    const stats = analyzer.getFieldStat('mixed');
    expect(stats).toBeDefined();

    const total = stats!.count;
    const stringPercent = (stats!.types.get('string') ?? 0) / total;
    expect(stringPercent).toBeCloseTo(0.3);
  });

  it('should identify the dominant type correctly', () => {
    analyzer.analyzeDocuments(createTypeDistributionDocuments());

    const stats = analyzer.getFieldStat('mixed');
    // String appears 3 times, most frequent
    expect(stats?.dominantType).toBe('string');
  });

  it('should mark inconsistent when multiple types present', () => {
    analyzer.analyzeDocuments(createTypeDistributionDocuments());

    const stats = analyzer.getFieldStat('mixed');
    expect(stats?.isConsistent).toBe(false);
  });

  it('should mark consistent when single non-null type', () => {
    analyzer.analyzeDocuments([
      { field: 'a' },
      { field: 'b' },
      { field: null },
      { field: 'c' },
    ]);

    const stats = analyzer.getFieldStat('field');
    // Only string type (ignoring null for consistency)
    expect(stats?.isConsistent).toBe(true);
    expect(stats?.dominantType).toBe('string');
  });

  it('should handle all-null fields correctly', () => {
    analyzer.analyzeDocuments([
      { field: null },
      { field: null },
      { field: null },
    ]);

    const stats = analyzer.getFieldStat('field');
    expect(stats?.dominantType).toBe('null');
    expect(stats?.types.get('null')).toBe(3);
    expect(stats?.isConsistent).toBe(true);
  });

  it('should track ObjectId type distribution', () => {
    analyzer.analyzeDocuments([
      { _id: new ObjectId() },
      { _id: new ObjectId() },
      { _id: 'string-id' },
    ]);

    const stats = analyzer.getFieldStat('_id');
    expect(stats?.types.get('objectId')).toBe(2);
    expect(stats?.types.get('string')).toBe(1);
    expect(stats?.dominantType).toBe('objectId');
  });
});

// ============================================================================
// Nested Pattern Identification Tests
// ============================================================================

describe('FieldAnalyzer nested pattern identification', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should identify deeply nested field paths', () => {
    analyzer.analyzeDocuments(createComplexNestedDocuments());

    const paths = analyzer.getFieldPaths();
    expect(paths).toContain('metadata.source.type');
    expect(paths).toContain('metadata.source.version');
    expect(paths).toContain('metadata.source.details.endpoint');
    expect(paths).toContain('metadata.timestamps.created');
  });

  it('should track nested field frequency across documents', () => {
    analyzer.analyzeDocuments(createComplexNestedDocuments());

    // metadata.source.type appears in all 3 docs
    const typeStats = analyzer.getFieldStat('metadata.source.type');
    expect(typeStats?.frequency).toBe(1.0);

    // metadata.source.details only appears in 1 doc
    const detailsStats = analyzer.getFieldStat('metadata.source.details');
    expect(detailsStats?.frequency).toBeCloseTo(1 / 3);
  });

  it('should mark nested fields with isNested flag', () => {
    analyzer.analyzeDocuments(createComplexNestedDocuments());

    const rootStats = analyzer.getFieldStat('metadata');
    expect(rootStats?.isNested).toBe(false);

    const nestedStats = analyzer.getFieldStat('metadata.source.type');
    expect(nestedStats?.isNested).toBe(true);
  });

  it('should handle arrays of objects with [] notation', () => {
    analyzer.analyzeDocuments(createComplexNestedDocuments());

    const paths = analyzer.getFieldPaths();
    // Array element structure should be analyzed
    expect(paths).toContain('data.users');
    expect(paths).toContain('data.users[].id');
    expect(paths).toContain('data.users[].name');
  });

  it('should track types within array element structures', () => {
    analyzer.analyzeDocuments(createComplexNestedDocuments());

    const idStats = analyzer.getFieldStat('data.users[].id');
    expect(idStats?.dominantType).toBe('number');

    const nameStats = analyzer.getFieldStat('data.users[].name');
    expect(nameStats?.dominantType).toBe('string');
  });

  it('should respect maxDepth for deeply nested structures', () => {
    const deepAnalyzer = new FieldAnalyzer({ maxDepth: 2 });
    deepAnalyzer.analyzeDocuments(createComplexNestedDocuments());

    const paths = deepAnalyzer.getFieldPaths();
    // Should stop at depth 2 - depth counts from 0 at root
    // depth 0: metadata, data
    // depth 1: metadata.source, metadata.timestamps, data.users, data.events
    // depth 2: metadata.source.type, metadata.source.version, etc.
    expect(paths).toContain('metadata');
    expect(paths).toContain('metadata.source');
    expect(paths).toContain('metadata.source.type');
    // Depth 3+ should not be included
    expect(paths).not.toContain('metadata.source.details.endpoint');
    expect(paths).not.toContain('metadata.source.details.method');
  });

  it('should identify common nested patterns across documents', () => {
    analyzer.analyzeDocuments([
      { user: { name: 'Alice', email: 'alice@example.com' } },
      { user: { name: 'Bob', email: 'bob@example.com' } },
      { user: { name: 'Charlie', phone: '123-456' } },
    ]);

    // All users have name
    const nameStats = analyzer.getFieldStat('user.name');
    expect(nameStats?.frequency).toBe(1.0);

    // 2/3 users have email
    const emailStats = analyzer.getFieldStat('user.email');
    expect(emailStats?.frequency).toBeCloseTo(2 / 3);

    // 1/3 users have phone
    const phoneStats = analyzer.getFieldStat('user.phone');
    expect(phoneStats?.frequency).toBeCloseTo(1 / 3);
  });
});

// ============================================================================
// Occurrence Frequency Calculation Tests
// ============================================================================

describe('FieldAnalyzer occurrence frequency calculation', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should calculate exact frequency as count/total', () => {
    analyzer.analyzeDocuments(createFieldPresenceDocuments());

    expect(analyzer.getDocumentCount()).toBe(10);

    const alwaysStats = analyzer.getFieldStat('always');
    expect(alwaysStats?.frequency).toBe(10 / 10);

    const sometimesStats = analyzer.getFieldStat('sometimes');
    expect(sometimesStats?.frequency).toBe(3 / 10);

    const rarelyStats = analyzer.getFieldStat('rarely');
    expect(rarelyStats?.frequency).toBe(1 / 10);
  });

  it('should maintain frequency accuracy after incremental analysis', () => {
    // Analyze in batches
    analyzer.analyzeDocuments([
      { field: 'a' },
      { field: 'b' },
    ]);
    analyzer.analyzeDocuments([
      { field: 'c' },
      {},
      {},
    ]);

    expect(analyzer.getDocumentCount()).toBe(5);

    const stats = analyzer.getFieldStat('field');
    expect(stats?.count).toBe(3);
    expect(stats?.frequency).toBe(0.6);
  });

  it('should calculate frequency for fields appearing in edge cases', () => {
    analyzer.analyzeDocuments([{ field: 'only-one' }]);

    const stats = analyzer.getFieldStat('field');
    expect(stats?.frequency).toBe(1.0);
  });

  it('should handle frequency calculation with large document counts', () => {
    const manyDocs: Record<string, unknown>[] = [];
    for (let i = 0; i < 1000; i++) {
      // common appears in all docs
      // rare appears only in first 10 docs (not as undefined which would count)
      if (i < 10) {
        manyDocs.push({ common: i, rare: i });
      } else {
        manyDocs.push({ common: i });
      }
    }

    analyzer.analyzeDocuments(manyDocs);

    const commonStats = analyzer.getFieldStat('common');
    expect(commonStats?.frequency).toBe(1.0);

    const rareStats = analyzer.getFieldStat('rare');
    expect(rareStats?.frequency).toBe(0.01); // 10/1000
  });

  it('should return 0 frequency when no documents analyzed', () => {
    const emptyAnalyzer = new FieldAnalyzer();
    const stats = emptyAnalyzer.getFieldStats();

    expect(emptyAnalyzer.getDocumentCount()).toBe(0);
    expect(stats.size).toBe(0);
  });

  it('should support frequency-based field filtering', () => {
    analyzer.analyzeDocuments(createFieldPresenceDocuments());

    const stats = analyzer.getFieldStats();
    const highFrequencyFields = Array.from(stats.entries())
      .filter(([_, stat]) => stat.frequency >= 0.5)
      .map(([path]) => path);

    expect(highFrequencyFields).toContain('always');
    expect(highFrequencyFields).not.toContain('sometimes');
    expect(highFrequencyFields).not.toContain('rarely');
  });
});

// ============================================================================
// Null and Missing Value Handling Tests
// ============================================================================

describe('FieldAnalyzer null and missing value handling', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should count null as a valid field presence', () => {
    analyzer.analyzeDocuments(createNullMissingDocuments());

    const nullableStats = analyzer.getFieldStat('nullable');
    // nullable appears in 3 documents (including null values)
    expect(nullableStats?.count).toBe(3);
  });

  it('should track null type separately in distribution', () => {
    analyzer.analyzeDocuments(createNullMissingDocuments());

    const nullableStats = analyzer.getFieldStat('nullable');
    expect(nullableStats?.types.get('null')).toBe(2);
    expect(nullableStats?.types.get('string')).toBe(1);
  });

  it('should treat undefined as null type', () => {
    analyzer.analyzeDocuments([
      { field: undefined },
      { field: 'value' },
    ]);

    const stats = analyzer.getFieldStat('field');
    expect(stats?.types.get('null')).toBe(1);
    expect(stats?.types.get('string')).toBe(1);
  });

  it('should not include missing fields in count', () => {
    analyzer.analyzeDocuments([
      { a: 1, b: 2 },
      { a: 2 }, // b is missing
      { a: 3, b: null }, // b is present but null
    ]);

    const bStats = analyzer.getFieldStat('b');
    // b is present in 2 documents (doc 1 and doc 3)
    expect(bStats?.count).toBe(2);
    expect(bStats?.frequency).toBeCloseTo(2 / 3);
  });

  it('should calculate dominant type excluding nulls when other types present', () => {
    analyzer.analyzeDocuments([
      { field: null },
      { field: null },
      { field: 'value' },
    ]);

    const stats = analyzer.getFieldStat('field');
    // string is dominant (nulls excluded from dominance calculation)
    expect(stats?.dominantType).toBe('string');
    expect(stats?.types.get('null')).toBe(2);
    expect(stats?.types.get('string')).toBe(1);
  });

  it('should handle fields with only null/undefined values', () => {
    analyzer.analyzeDocuments([
      { field: null },
      { field: undefined },
      { field: null },
    ]);

    const stats = analyzer.getFieldStat('field');
    expect(stats?.dominantType).toBe('null');
    expect(stats?.isConsistent).toBe(true);
  });

  it('should exclude null values from sample values', () => {
    analyzer.analyzeDocuments([
      { field: null },
      { field: 'actual-value' },
      { field: null },
    ]);

    const stats = analyzer.getFieldStat('field');
    expect(stats?.sampleValues).not.toContain(null);
    expect(stats?.sampleValues).toContain('actual-value');
  });

  it('should handle sparse documents with many missing fields', () => {
    analyzer.analyzeDocuments([
      { a: 1 },
      { b: 2 },
      { c: 3 },
      { a: 4, b: 5, c: 6 },
    ]);

    expect(analyzer.getFieldStat('a')?.frequency).toBe(0.5);
    expect(analyzer.getFieldStat('b')?.frequency).toBe(0.5);
    expect(analyzer.getFieldStat('c')?.frequency).toBe(0.5);
  });
});

// ============================================================================
// Promotion Suggestion Tests with Null Handling
// ============================================================================

describe('FieldAnalyzer promotion suggestions with null handling', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should not suggest promotion for null-only fields', () => {
    analyzer.analyzeDocuments([
      { field: null },
      { field: null },
      { field: null },
    ]);

    const suggestions = analyzer.suggestPromotions(0.5);
    expect(suggestions.find(s => s.path === 'field')).toBeUndefined();
  });

  it('should suggest promotion based on non-null type distribution', () => {
    analyzer.analyzeDocuments([
      { field: 'value1' },
      { field: null },
      { field: 'value2' },
      { field: null },
      { field: 'value3' },
    ]);

    const suggestions = analyzer.suggestPromotions(0.5);
    const fieldSuggestion = suggestions.find(s => s.path === 'field');

    expect(fieldSuggestion).toBeDefined();
    expect(fieldSuggestion?.suggestedType).toBe('string');
    // Confidence should be based on non-null values (3/3 strings = 100%)
    expect(fieldSuggestion?.confidence).toBe(1.0);
  });

  it('should calculate confidence correctly with mixed types', () => {
    analyzer.analyzeDocuments([
      { field: 'string' },
      { field: 'string' },
      { field: 123 },
      { field: null },
    ]);

    const suggestions = analyzer.suggestPromotions(0.5, { minConsistency: 0 });
    const fieldSuggestion = suggestions.find(s => s.path === 'field');

    // 2 strings, 1 number (null excluded) = 2/3 = 66.7% confidence
    expect(fieldSuggestion?.confidence).toBeCloseTo(2 / 3);
  });

  it('should handle fields with high null ratio but consistent non-null type', () => {
    analyzer.analyzeDocuments([
      { field: null },
      { field: null },
      { field: null },
      { field: null },
      { field: 'value' },
    ]);

    const suggestions = analyzer.suggestPromotions(0.5);
    const fieldSuggestion = suggestions.find(s => s.path === 'field');

    // Field appears 100% of time, non-null type is 100% consistent
    expect(fieldSuggestion).toBeDefined();
    expect(fieldSuggestion?.suggestedType).toBe('string');
    expect(fieldSuggestion?.confidence).toBe(1.0);
  });
});

// ============================================================================
// Merge Behavior with Null Handling Tests
// ============================================================================

describe('FieldAnalyzer merge with null handling', () => {
  it('should correctly merge null type counts', () => {
    const analyzer1 = new FieldAnalyzer();
    analyzer1.analyzeDocuments([
      { field: null },
      { field: 'value1' },
    ]);

    const analyzer2 = new FieldAnalyzer();
    analyzer2.analyzeDocuments([
      { field: null },
      { field: 'value2' },
    ]);

    analyzer1.merge(analyzer2);

    const stats = analyzer1.getFieldStat('field');
    expect(stats?.count).toBe(4);
    expect(stats?.types.get('null')).toBe(2);
    expect(stats?.types.get('string')).toBe(2);
  });

  it('should maintain correct frequency after merge', () => {
    const analyzer1 = new FieldAnalyzer();
    analyzer1.analyzeDocuments([
      { field: 'a' },
      {},
    ]);

    const analyzer2 = new FieldAnalyzer();
    analyzer2.analyzeDocuments([
      { field: 'b' },
      { field: 'c' },
      {},
    ]);

    analyzer1.merge(analyzer2);

    expect(analyzer1.getDocumentCount()).toBe(5);
    const stats = analyzer1.getFieldStat('field');
    expect(stats?.count).toBe(3);
    expect(stats?.frequency).toBe(0.6);
  });
});

// ============================================================================
// Edge Cases and Error Handling Tests
// ============================================================================

describe('FieldAnalyzer edge cases', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should handle documents with circular reference patterns gracefully', () => {
    // Note: This tests that we don't infinite loop, not that we support circular refs
    const doc: Record<string, unknown> = { name: 'test' };
    // In JS, you can't create true circular refs in a plain object literal
    // but we test that deeply nested objects don't cause issues

    const deepDoc: Record<string, unknown> = {
      level1: {
        level2: {
          level3: {
            level4: {
              level5: {
                level6: {
                  level7: {
                    level8: {
                      level9: {
                        level10: {
                          level11: {
                            value: 'deep',
                          },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
    };

    // Should not throw and should respect maxDepth
    expect(() => analyzer.analyzeDocuments([deepDoc])).not.toThrow();
  });

  it('should handle empty objects in nested structures', () => {
    analyzer.analyzeDocuments([
      { nested: {} },
      { nested: { value: 'present' } },
    ]);

    const nestedStats = analyzer.getFieldStat('nested');
    expect(nestedStats?.count).toBe(2);
    expect(nestedStats?.dominantType).toBe('object');

    const valueStats = analyzer.getFieldStat('nested.value');
    expect(valueStats?.count).toBe(1);
    expect(valueStats?.frequency).toBe(0.5);
  });

  it('should handle arrays with mixed element types', () => {
    analyzer.analyzeDocuments([
      { items: [1, 'two', true, null] },
      { items: [{ nested: 'object' }] },
    ]);

    const stats = analyzer.getFieldStat('items');
    expect(stats?.dominantType).toBe('array');
    expect(stats?.count).toBe(2);
  });

  it('should handle special numeric values', () => {
    analyzer.analyzeDocuments([
      { num: NaN },
      { num: Infinity },
      { num: -Infinity },
      { num: 0 },
      { num: -0 },
    ]);

    const stats = analyzer.getFieldStat('num');
    expect(stats?.dominantType).toBe('number');
    expect(stats?.count).toBe(5);
  });

  it('should handle symbol keys gracefully', () => {
    const sym = Symbol('test');
    const doc: Record<string | symbol, unknown> = {
      normalKey: 'value',
      [sym]: 'symbol-value', // This won't be iterable via Object.entries
    };

    // Should not throw
    expect(() => analyzer.analyzeDocuments([doc as Record<string, unknown>])).not.toThrow();

    const stats = analyzer.getFieldStat('normalKey');
    expect(stats).toBeDefined();
  });

  it('should handle very long field names', () => {
    const longName = 'a'.repeat(1000);
    analyzer.analyzeDocuments([{ [longName]: 'value' }]);

    const stats = analyzer.getFieldStat(longName);
    expect(stats).toBeDefined();
    expect(stats?.dominantType).toBe('string');
  });

  it('should handle fields with dots in names', () => {
    // Note: This is tricky because dots are used for path notation
    analyzer.analyzeDocuments([
      { 'field.with.dots': 'value' },
    ]);

    // The field name itself contains dots, which may conflict with path notation
    // This test documents expected behavior
    const stats = analyzer.getFieldStat('field.with.dots');
    expect(stats).toBeDefined();
  });

  it('should handle binary data correctly', () => {
    const buffer = new Uint8Array([0x00, 0xFF, 0x42, 0x13, 0x37]);
    analyzer.analyzeDocuments([
      { data: buffer },
      { data: new Uint8Array([1, 2, 3]) },
    ]);

    const stats = analyzer.getFieldStat('data');
    expect(stats?.dominantType).toBe('binary');
    expect(stats?.count).toBe(2);
    expect(stats?.isConsistent).toBe(true);
  });
});

// ============================================================================
// Performance and Scale Tests
// ============================================================================

describe('FieldAnalyzer performance', () => {
  it('should handle analysis of many documents efficiently', () => {
    const analyzer = new FieldAnalyzer();
    const docs: Record<string, unknown>[] = [];

    for (let i = 0; i < 10000; i++) {
      docs.push({
        id: i,
        name: `User ${i}`,
        active: i % 2 === 0,
        score: Math.random() * 100,
        tags: ['tag1', 'tag2'],
        metadata: {
          created: new Date(),
          version: i % 10,
        },
      });
    }

    const start = performance.now();
    analyzer.analyzeDocuments(docs);
    const duration = performance.now() - start;

    // Should complete in reasonable time (< 1 second for 10k docs)
    expect(duration).toBeLessThan(1000);
    expect(analyzer.getDocumentCount()).toBe(10000);
  });

  it('should handle many unique fields efficiently', () => {
    const analyzer = new FieldAnalyzer();
    const doc: Record<string, unknown> = {};

    for (let i = 0; i < 1000; i++) {
      doc[`field_${i}`] = `value_${i}`;
    }

    const start = performance.now();
    analyzer.analyzeDocuments([doc]);
    const duration = performance.now() - start;

    expect(duration).toBeLessThan(100);
    expect(analyzer.getFieldPaths().length).toBe(1000);
  });
});

// ============================================================================
// RED Phase: Failing Tests for Features That Should Be Added
// These tests are intentionally written to FAIL until the implementation is done
// ============================================================================

describe('FieldAnalyzer.getTypeDistribution (RED - not yet implemented)', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should return percentage distribution of types for a field', () => {
    analyzer.analyzeDocuments(createTypeDistributionDocuments());

    // RED: This method doesn't exist yet - test should fail
    // getTypeDistribution should return a map of type -> percentage
    expect(typeof (analyzer as unknown as Record<string, unknown>).getTypeDistribution).toBe('function');

    const distribution = (analyzer as unknown as {
      getTypeDistribution: (path: string) => Map<DetectedType, number>;
    }).getTypeDistribution('mixed');

    expect(distribution.get('string')).toBeCloseTo(0.3);
    expect(distribution.get('number')).toBeCloseTo(0.2);
    expect(distribution.get('boolean')).toBeCloseTo(0.1);
  });
});

describe('FieldAnalyzer.getNullRatio (RED - not yet implemented)', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should return the ratio of null values for a field', () => {
    analyzer.analyzeDocuments([
      { field: null },
      { field: null },
      { field: 'value' },
      { field: 'another' },
    ]);

    // RED: This method doesn't exist yet - test should fail
    // getNullRatio should return the percentage of null values
    expect(typeof (analyzer as unknown as Record<string, unknown>).getNullRatio).toBe('function');

    const nullRatio = (analyzer as unknown as {
      getNullRatio: (path: string) => number;
    }).getNullRatio('field');

    expect(nullRatio).toBe(0.5); // 2 nulls out of 4
  });
});

describe('FieldAnalyzer.getMissingRatio (RED - not yet implemented)', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should return the ratio of documents missing a field', () => {
    analyzer.analyzeDocuments([
      { field: 'value' },
      { other: 'data' },
      { field: 'another' },
      { other: 'more' },
    ]);

    // RED: This method doesn't exist yet - test should fail
    // getMissingRatio should return 1 - frequency
    expect(typeof (analyzer as unknown as Record<string, unknown>).getMissingRatio).toBe('function');

    const missingRatio = (analyzer as unknown as {
      getMissingRatio: (path: string) => number;
    }).getMissingRatio('field');

    expect(missingRatio).toBe(0.5); // 2 missing out of 4
  });
});

describe('FieldAnalyzer.getFieldsByPattern (RED - not yet implemented)', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should return fields matching a glob pattern', () => {
    analyzer.analyzeDocuments([
      {
        user: {
          name: 'Alice',
          email: 'alice@test.com',
          profile: { bio: 'Hello' },
        },
        meta: { version: 1 },
      },
    ]);

    // RED: This method doesn't exist yet - test should fail
    // getFieldsByPattern should return fields matching pattern
    expect(typeof (analyzer as unknown as Record<string, unknown>).getFieldsByPattern).toBe('function');

    const userFields = (analyzer as unknown as {
      getFieldsByPattern: (pattern: string) => string[];
    }).getFieldsByPattern('user.*');

    expect(userFields).toContain('user.name');
    expect(userFields).toContain('user.email');
    expect(userFields).toContain('user.profile');
    expect(userFields).not.toContain('meta.version');
  });
});

describe('FieldAnalyzer.getNestedFields (RED - not yet implemented)', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should return only nested fields (depth > 0)', () => {
    analyzer.analyzeDocuments([
      {
        name: 'test',
        user: {
          profile: { age: 30 },
        },
      },
    ]);

    // RED: This method doesn't exist yet - test should fail
    // getNestedFields should return only fields with dots in path
    expect(typeof (analyzer as unknown as Record<string, unknown>).getNestedFields).toBe('function');

    const nestedFields = (analyzer as unknown as {
      getNestedFields: () => string[];
    }).getNestedFields();

    expect(nestedFields).toContain('user.profile');
    expect(nestedFields).toContain('user.profile.age');
    expect(nestedFields).not.toContain('name');
    expect(nestedFields).not.toContain('user');
  });
});

describe('FieldAnalyzer.getRootFields (RED - not yet implemented)', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should return only root-level fields', () => {
    analyzer.analyzeDocuments([
      {
        name: 'test',
        age: 30,
        user: {
          profile: { bio: 'Hello' },
        },
      },
    ]);

    // RED: This method doesn't exist yet - test should fail
    // getRootFields should return only top-level fields
    expect(typeof (analyzer as unknown as Record<string, unknown>).getRootFields).toBe('function');

    const rootFields = (analyzer as unknown as {
      getRootFields: () => string[];
    }).getRootFields();

    expect(rootFields).toContain('name');
    expect(rootFields).toContain('age');
    expect(rootFields).toContain('user');
    expect(rootFields).not.toContain('user.profile');
    expect(rootFields).not.toContain('user.profile.bio');
  });
});

describe('FieldAnalyzer.toJSON (RED - not yet implemented)', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should serialize analyzer state to JSON', () => {
    analyzer.analyzeDocuments([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ]);

    // RED: This method doesn't exist yet - test should fail
    // toJSON should return serializable state
    expect(typeof (analyzer as unknown as Record<string, unknown>).toJSON).toBe('function');

    const json = (analyzer as unknown as {
      toJSON: () => Record<string, unknown>;
    }).toJSON();

    expect(json).toHaveProperty('documentCount', 2);
    expect(json).toHaveProperty('fields');
    expect(typeof json.fields).toBe('object');
  });
});

describe('FieldAnalyzer.fromJSON (RED - not yet implemented)', () => {
  it('should deserialize analyzer state from JSON', () => {
    // RED: This static method doesn't exist yet - test should fail
    // fromJSON should restore analyzer state
    expect(typeof (FieldAnalyzer as unknown as Record<string, unknown>).fromJSON).toBe('function');

    const analyzer1 = new FieldAnalyzer();
    analyzer1.analyzeDocuments([{ name: 'Test' }]);

    const json = (analyzer1 as unknown as {
      toJSON: () => Record<string, unknown>;
    }).toJSON();

    const analyzer2 = (FieldAnalyzer as unknown as {
      fromJSON: (data: Record<string, unknown>) => FieldAnalyzer;
    }).fromJSON(json);

    expect(analyzer2.getDocumentCount()).toBe(1);
    expect(analyzer2.getFieldStat('name')).toBeDefined();
  });
});

describe('FieldAnalyzer.getFieldsAtDepth (RED - not yet implemented)', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should return fields at a specific nesting depth', () => {
    analyzer.analyzeDocuments([
      {
        a: 1,
        b: {
          c: 2,
          d: {
            e: 3,
          },
        },
      },
    ]);

    // RED: This method doesn't exist yet - test should fail
    // getFieldsAtDepth should return fields at specific depth
    expect(typeof (analyzer as unknown as Record<string, unknown>).getFieldsAtDepth).toBe('function');

    const depth0 = (analyzer as unknown as {
      getFieldsAtDepth: (depth: number) => string[];
    }).getFieldsAtDepth(0);

    expect(depth0).toEqual(['a', 'b']);

    const depth1 = (analyzer as unknown as {
      getFieldsAtDepth: (depth: number) => string[];
    }).getFieldsAtDepth(1);

    expect(depth1).toEqual(['b.c', 'b.d']);

    const depth2 = (analyzer as unknown as {
      getFieldsAtDepth: (depth: number) => string[];
    }).getFieldsAtDepth(2);

    expect(depth2).toEqual(['b.d.e']);
  });
});

describe('FieldAnalyzer.getArrayElementStats (RED - not yet implemented)', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should return statistics about array elements', () => {
    analyzer.analyzeDocuments([
      { items: [1, 2, 3] },
      { items: [4, 5] },
      { items: [] },
      { items: [6, 7, 8, 9] },
    ]);

    // RED: This method doesn't exist yet - test should fail
    // getArrayElementStats should return element-level stats
    expect(typeof (analyzer as unknown as Record<string, unknown>).getArrayElementStats).toBe('function');

    const stats = (analyzer as unknown as {
      getArrayElementStats: (path: string) => {
        avgLength: number;
        minLength: number;
        maxLength: number;
        elementType: DetectedType;
      };
    }).getArrayElementStats('items');

    expect(stats.avgLength).toBeCloseTo(2.25); // (3+2+0+4)/4
    expect(stats.minLength).toBe(0);
    expect(stats.maxLength).toBe(4);
    expect(stats.elementType).toBe('number');
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('FieldAnalyzer integration', () => {
  it('should provide complete field analysis for realistic MongoDB documents', () => {
    const documents = [
      {
        _id: new ObjectId(),
        username: 'alice',
        email: 'alice@example.com',
        profile: {
          firstName: 'Alice',
          lastName: 'Smith',
          age: 30,
          address: {
            city: 'New York',
            country: 'USA',
          },
        },
        roles: ['user', 'admin'],
        createdAt: new Date('2024-01-01'),
        updatedAt: new Date('2024-01-15'),
        lastLogin: new Date('2024-02-01'),
        settings: {
          theme: 'dark',
          notifications: true,
        },
      },
      {
        _id: new ObjectId(),
        username: 'bob',
        email: 'bob@example.com',
        profile: {
          firstName: 'Bob',
          lastName: 'Jones',
          age: 25,
        },
        roles: ['user'],
        createdAt: new Date('2024-01-10'),
        updatedAt: new Date('2024-01-20'),
        settings: {
          theme: 'light',
        },
      },
      {
        _id: new ObjectId(),
        username: 'charlie',
        email: null, // Explicitly null
        profile: {
          firstName: 'Charlie',
          lastName: 'Brown',
        },
        roles: ['user', 'moderator'],
        createdAt: new Date('2024-01-15'),
        verified: true,
      },
    ];

    const analyzer = new FieldAnalyzer();
    analyzer.analyzeDocuments(documents);

    // Test field presence
    expect(analyzer.getFieldStat('_id')?.frequency).toBe(1.0);
    expect(analyzer.getFieldStat('username')?.frequency).toBe(1.0);
    expect(analyzer.getFieldStat('email')?.frequency).toBe(1.0); // null counts as present
    expect(analyzer.getFieldStat('lastLogin')?.frequency).toBeCloseTo(1 / 3);
    expect(analyzer.getFieldStat('verified')?.frequency).toBeCloseTo(1 / 3);

    // Test type detection
    expect(analyzer.getFieldStat('_id')?.dominantType).toBe('objectId');
    expect(analyzer.getFieldStat('username')?.dominantType).toBe('string');
    expect(analyzer.getFieldStat('profile.age')?.dominantType).toBe('number');
    expect(analyzer.getFieldStat('roles')?.dominantType).toBe('array');
    expect(analyzer.getFieldStat('createdAt')?.dominantType).toBe('date');
    expect(analyzer.getFieldStat('settings.notifications')?.dominantType).toBe('boolean');

    // Test nested pattern detection
    const paths = analyzer.getFieldPaths();
    expect(paths).toContain('profile.firstName');
    expect(paths).toContain('profile.address.city');
    expect(paths).toContain('settings.theme');

    // Test promotion suggestions
    const suggestions = analyzer.suggestPromotions(0.9);
    const suggestedPaths = suggestions.map(s => s.path);

    // High frequency fields should be suggested
    expect(suggestedPaths).toContain('_id');
    expect(suggestedPaths).toContain('username');
    expect(suggestedPaths).toContain('profile.firstName');
    expect(suggestedPaths).toContain('profile.lastName');

    // Low frequency fields should not be suggested
    expect(suggestedPaths).not.toContain('lastLogin');
    expect(suggestedPaths).not.toContain('verified');
    expect(suggestedPaths).not.toContain('profile.address.city');
  });

  it('should support incremental analysis workflow', () => {
    const analyzer = new FieldAnalyzer();

    // Batch 1: Initial documents
    analyzer.analyzeDocuments([
      { type: 'A', value: 100 },
      { type: 'A', value: 200 },
    ]);

    expect(analyzer.getDocumentCount()).toBe(2);
    expect(analyzer.getFieldStat('type')?.frequency).toBe(1.0);

    // Batch 2: More documents with schema evolution
    analyzer.analyzeDocuments([
      { type: 'B', value: 300, newField: 'added' },
      { type: 'B', value: 400, newField: 'also added' },
      { type: 'C', value: 500 },
    ]);

    expect(analyzer.getDocumentCount()).toBe(5);
    expect(analyzer.getFieldStat('type')?.frequency).toBe(1.0);
    expect(analyzer.getFieldStat('newField')?.frequency).toBe(0.4);

    // Promotion suggestions should reflect cumulative analysis
    const suggestions = analyzer.suggestPromotions(0.8);
    expect(suggestions.some(s => s.path === 'type')).toBe(true);
    expect(suggestions.some(s => s.path === 'value')).toBe(true);
    expect(suggestions.some(s => s.path === 'newField')).toBe(false);
  });
});
