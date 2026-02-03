/**
 * Field Analyzer Tests
 *
 * Tests for document field analysis including:
 * - Type detection for various field types
 * - Nested field analysis with dot notation
 * - Field frequency tracking across documents
 * - Promotion suggestions based on threshold
 * - Analyzer merging and reset
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  FieldAnalyzer,
  createFieldAnalyzer,
  analyzeDocuments,
  suggestFieldPromotions,
} from '../analyzer.js';
import type {
  FieldStats,
  PromotionSuggestion,
  DetectedType,
} from '../analyzer.js';
import { ObjectId } from '../../types.js';

// ============================================================================
// Test Fixtures
// ============================================================================

function createSimpleDocuments(): Record<string, unknown>[] {
  return [
    { name: 'Alice', age: 30, email: 'alice@example.com' },
    { name: 'Bob', age: 25 },
    { name: 'Charlie', age: 35, email: 'charlie@example.com' },
  ];
}

function createNestedDocuments(): Record<string, unknown>[] {
  return [
    {
      user: {
        profile: {
          name: 'Alice',
          bio: 'Developer',
        },
        settings: {
          theme: 'dark',
        },
      },
    },
    {
      user: {
        profile: {
          name: 'Bob',
        },
        settings: {
          theme: 'light',
          notifications: true,
        },
      },
    },
  ];
}

function createMixedTypeDocuments(): Record<string, unknown>[] {
  return [
    { value: 'string' },
    { value: 123 },
    { value: true },
    { value: 'another string' },
    { value: 456 },
  ];
}

function createArrayDocuments(): Record<string, unknown>[] {
  return [
    { tags: ['javascript', 'typescript'], scores: [85, 90, 95] },
    { tags: ['python'], scores: [80] },
    { tags: ['go', 'rust', 'c++'], scores: [92, 88] },
  ];
}

// ============================================================================
// Constructor Tests
// ============================================================================

describe('FieldAnalyzer', () => {
  describe('constructor', () => {
    it('should create analyzer with default options', () => {
      const analyzer = new FieldAnalyzer();

      expect(analyzer.getDocumentCount()).toBe(0);
      expect(analyzer.getFieldPaths()).toEqual([]);
    });

    it('should create analyzer with custom options', () => {
      const analyzer = new FieldAnalyzer({
        maxDepth: 5,
        maxSamples: 3,
        excludeFields: ['_internal*'],
      });

      expect(analyzer.getDocumentCount()).toBe(0);
    });
  });
});

// ============================================================================
// Type Detection Tests
// ============================================================================

describe('FieldAnalyzer type detection', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should detect string type', () => {
    analyzer.analyzeDocuments([{ field: 'hello' }]);

    const stats = analyzer.getFieldStat('field');
    expect(stats?.dominantType).toBe('string');
    expect(stats?.isConsistent).toBe(true);
  });

  it('should detect number type', () => {
    analyzer.analyzeDocuments([
      { field: 42 },
      { field: 3.14 },
      { field: -100 },
    ]);

    const stats = analyzer.getFieldStat('field');
    expect(stats?.dominantType).toBe('number');
    expect(stats?.isConsistent).toBe(true);
  });

  it('should detect boolean type', () => {
    analyzer.analyzeDocuments([
      { field: true },
      { field: false },
    ]);

    const stats = analyzer.getFieldStat('field');
    expect(stats?.dominantType).toBe('boolean');
    expect(stats?.isConsistent).toBe(true);
  });

  it('should detect date type', () => {
    analyzer.analyzeDocuments([
      { field: new Date('2024-01-01') },
      { field: new Date() },
    ]);

    const stats = analyzer.getFieldStat('field');
    expect(stats?.dominantType).toBe('date');
    expect(stats?.isConsistent).toBe(true);
  });

  it('should detect binary type', () => {
    analyzer.analyzeDocuments([
      { field: new Uint8Array([1, 2, 3]) },
    ]);

    const stats = analyzer.getFieldStat('field');
    expect(stats?.dominantType).toBe('binary');
  });

  it('should detect array type', () => {
    analyzer.analyzeDocuments([
      { field: [1, 2, 3] },
      { field: ['a', 'b'] },
    ]);

    const stats = analyzer.getFieldStat('field');
    expect(stats?.dominantType).toBe('array');
    expect(stats?.isConsistent).toBe(true);
  });

  it('should detect object type', () => {
    analyzer.analyzeDocuments([
      { field: { nested: 'value' } },
    ]);

    const stats = analyzer.getFieldStat('field');
    expect(stats?.dominantType).toBe('object');
  });

  it('should detect null type', () => {
    analyzer.analyzeDocuments([
      { field: null },
    ]);

    const stats = analyzer.getFieldStat('field');
    expect(stats?.dominantType).toBe('null');
  });

  it('should detect ObjectId type', () => {
    analyzer.analyzeDocuments([
      { _id: new ObjectId() },
      { _id: new ObjectId() },
    ]);

    const stats = analyzer.getFieldStat('_id');
    expect(stats?.dominantType).toBe('objectId');
    expect(stats?.isConsistent).toBe(true);
  });

  it('should handle mixed types and identify dominant', () => {
    analyzer.analyzeDocuments(createMixedTypeDocuments());

    const stats = analyzer.getFieldStat('value');
    // 2 strings, 2 numbers, 1 boolean - strings are dominant
    expect(stats?.types.get('string')).toBe(2);
    expect(stats?.types.get('number')).toBe(2);
    expect(stats?.types.get('boolean')).toBe(1);
    expect(stats?.isConsistent).toBe(false);
  });
});

// ============================================================================
// Nested Field Analysis Tests
// ============================================================================

describe('FieldAnalyzer nested field analysis', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should analyze nested fields with dot notation', () => {
    analyzer.analyzeDocuments(createNestedDocuments());

    const paths = analyzer.getFieldPaths();
    expect(paths).toContain('user');
    expect(paths).toContain('user.profile');
    expect(paths).toContain('user.profile.name');
    expect(paths).toContain('user.profile.bio');
    expect(paths).toContain('user.settings');
    expect(paths).toContain('user.settings.theme');
    expect(paths).toContain('user.settings.notifications');
  });

  it('should track frequency for nested fields', () => {
    analyzer.analyzeDocuments(createNestedDocuments());

    const nameStats = analyzer.getFieldStat('user.profile.name');
    expect(nameStats?.count).toBe(2);
    expect(nameStats?.frequency).toBe(1);

    const bioStats = analyzer.getFieldStat('user.profile.bio');
    expect(bioStats?.count).toBe(1);
    expect(bioStats?.frequency).toBe(0.5);

    const notificationsStats = analyzer.getFieldStat('user.settings.notifications');
    expect(notificationsStats?.count).toBe(1);
    expect(notificationsStats?.frequency).toBe(0.5);
  });

  it('should mark nested fields as nested', () => {
    analyzer.analyzeDocuments(createNestedDocuments());

    const nameStats = analyzer.getFieldStat('user.profile.name');
    expect(nameStats?.isNested).toBe(true);

    const userStats = analyzer.getFieldStat('user');
    expect(userStats?.isNested).toBe(false);
  });

  it('should respect maxDepth option', () => {
    const shallowAnalyzer = new FieldAnalyzer({ maxDepth: 1 });
    shallowAnalyzer.analyzeDocuments(createNestedDocuments());

    const paths = shallowAnalyzer.getFieldPaths();
    expect(paths).toContain('user');
    expect(paths).toContain('user.profile');
    expect(paths).toContain('user.settings');
    // Deeper paths should not be analyzed
    expect(paths).not.toContain('user.profile.name');
  });

  it('should analyze deeply nested structures', () => {
    const deepDoc = {
      level1: {
        level2: {
          level3: {
            level4: {
              level5: {
                value: 'deep',
              },
            },
          },
        },
      },
    };

    analyzer.analyzeDocuments([deepDoc]);

    const stats = analyzer.getFieldStat('level1.level2.level3.level4.level5.value');
    expect(stats?.dominantType).toBe('string');
  });
});

// ============================================================================
// Field Frequency Tests
// ============================================================================

describe('FieldAnalyzer frequency tracking', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should track document count', () => {
    analyzer.analyzeDocuments(createSimpleDocuments());

    expect(analyzer.getDocumentCount()).toBe(3);
  });

  it('should calculate field frequency', () => {
    analyzer.analyzeDocuments(createSimpleDocuments());

    const nameStats = analyzer.getFieldStat('name');
    expect(nameStats?.frequency).toBe(1); // All 3 docs have name

    const emailStats = analyzer.getFieldStat('email');
    expect(emailStats?.frequency).toBeCloseTo(2 / 3); // 2 of 3 docs have email
  });

  it('should accumulate across multiple analyzeDocuments calls', () => {
    analyzer.analyzeDocuments([{ a: 1 }]);
    analyzer.analyzeDocuments([{ a: 2 }, { b: 3 }]);

    expect(analyzer.getDocumentCount()).toBe(3);

    const aStats = analyzer.getFieldStat('a');
    expect(aStats?.count).toBe(2);
    expect(aStats?.frequency).toBeCloseTo(2 / 3);
  });

  it('should track field count correctly', () => {
    analyzer.analyzeDocuments(createSimpleDocuments());

    const ageStats = analyzer.getFieldStat('age');
    expect(ageStats?.count).toBe(3);
  });
});

// ============================================================================
// Sample Values Tests
// ============================================================================

describe('FieldAnalyzer sample values', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should collect sample values', () => {
    analyzer.analyzeDocuments(createSimpleDocuments());

    const nameStats = analyzer.getFieldStat('name');
    expect(nameStats?.sampleValues).toHaveLength(3);
    expect(nameStats?.sampleValues).toContain('Alice');
    expect(nameStats?.sampleValues).toContain('Bob');
    expect(nameStats?.sampleValues).toContain('Charlie');
  });

  it('should limit sample values to maxSamples', () => {
    const limitedAnalyzer = new FieldAnalyzer({ maxSamples: 2 });

    limitedAnalyzer.analyzeDocuments([
      { value: 'a' },
      { value: 'b' },
      { value: 'c' },
      { value: 'd' },
    ]);

    const stats = limitedAnalyzer.getFieldStat('value');
    expect(stats?.sampleValues).toHaveLength(2);
  });

  it('should deduplicate sample values', () => {
    analyzer.analyzeDocuments([
      { status: 'active' },
      { status: 'active' },
      { status: 'inactive' },
    ]);

    const stats = analyzer.getFieldStat('status');
    expect(stats?.sampleValues).toHaveLength(2);
    expect(stats?.sampleValues).toContain('active');
    expect(stats?.sampleValues).toContain('inactive');
  });

  it('should not include null in sample values', () => {
    analyzer.analyzeDocuments([
      { value: null },
      { value: 'actual' },
    ]);

    const stats = analyzer.getFieldStat('value');
    expect(stats?.sampleValues).not.toContain(null);
    expect(stats?.sampleValues).toContain('actual');
  });
});

// ============================================================================
// Field Exclusion Tests
// ============================================================================

describe('FieldAnalyzer field exclusion', () => {
  it('should exclude fields matching patterns', () => {
    const analyzer = new FieldAnalyzer({
      excludeFields: ['_*', 'internal*'],
    });

    analyzer.analyzeDocuments([
      { name: 'test', _secret: 'hidden', internalId: 123 },
    ]);

    const paths = analyzer.getFieldPaths();
    expect(paths).toContain('name');
    expect(paths).not.toContain('_secret');
    expect(paths).not.toContain('internalId');
  });

  it('should support glob patterns with wildcards', () => {
    const analyzer = new FieldAnalyzer({
      excludeFields: ['user.*.password'],
    });

    analyzer.analyzeDocuments([
      {
        user: {
          account: {
            password: 'secret',
            email: 'test@example.com',
          },
        },
      },
    ]);

    const paths = analyzer.getFieldPaths();
    expect(paths).toContain('user.account.email');
    expect(paths).not.toContain('user.account.password');
  });
});

// ============================================================================
// Promotion Suggestions Tests
// ============================================================================

describe('FieldAnalyzer.suggestPromotions', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should suggest fields above frequency threshold', () => {
    analyzer.analyzeDocuments(createSimpleDocuments());

    const suggestions = analyzer.suggestPromotions(0.5);

    // All fields should be suggested (name=100%, age=100%, email=67%)
    expect(suggestions).toHaveLength(3);
    expect(suggestions.map((s) => s.path)).toContain('name');
    expect(suggestions.map((s) => s.path)).toContain('age');
    expect(suggestions.map((s) => s.path)).toContain('email');
  });

  it('should filter fields below threshold', () => {
    analyzer.analyzeDocuments(createSimpleDocuments());

    const suggestions = analyzer.suggestPromotions(0.9);

    // Only name and age should be suggested (100%)
    expect(suggestions).toHaveLength(2);
    expect(suggestions.map((s) => s.path)).toContain('name');
    expect(suggestions.map((s) => s.path)).toContain('age');
    expect(suggestions.map((s) => s.path)).not.toContain('email');
  });

  it('should suggest correct Parquet types', () => {
    analyzer.analyzeDocuments(createSimpleDocuments());

    const suggestions = analyzer.suggestPromotions(0.5);

    const nameSuggestion = suggestions.find((s) => s.path === 'name');
    expect(nameSuggestion?.suggestedType).toBe('string');

    const ageSuggestion = suggestions.find((s) => s.path === 'age');
    expect(ageSuggestion?.suggestedType).toBe('double');
  });

  it('should include confidence based on type consistency', () => {
    analyzer.analyzeDocuments([
      { field: 'string' },
      { field: 'another' },
      { field: 123 }, // Inconsistent type
    ]);

    const suggestions = analyzer.suggestPromotions(0.5, { minConsistency: 0 });

    const fieldSuggestion = suggestions.find((s) => s.path === 'field');
    expect(fieldSuggestion?.confidence).toBeCloseTo(2 / 3);
  });

  it('should filter by minimum consistency', () => {
    analyzer.analyzeDocuments(createMixedTypeDocuments());

    // With high consistency requirement, mixed field should not be suggested
    const suggestions = analyzer.suggestPromotions(0.5, { minConsistency: 0.9 });

    expect(suggestions.find((s) => s.path === 'value')).toBeUndefined();
  });

  it('should sort by frequency descending', () => {
    analyzer.analyzeDocuments(createSimpleDocuments());

    const suggestions = analyzer.suggestPromotions(0.5);

    // First suggestions should have higher frequency
    expect(suggestions[0].frequency).toBeGreaterThanOrEqual(suggestions[1].frequency);
  });

  it('should respect maxSuggestions option', () => {
    analyzer.analyzeDocuments(createSimpleDocuments());

    const suggestions = analyzer.suggestPromotions(0.5, { maxSuggestions: 2 });

    expect(suggestions).toHaveLength(2);
  });

  it('should exclude array fields when requested', () => {
    analyzer.analyzeDocuments(createArrayDocuments());

    const suggestionsWithArrays = analyzer.suggestPromotions(0.5, { excludeArrays: false });
    expect(suggestionsWithArrays.some((s) => s.path === 'tags')).toBe(true);

    const suggestionsWithoutArrays = analyzer.suggestPromotions(0.5, { excludeArrays: true });
    expect(suggestionsWithoutArrays.some((s) => s.path === 'tags')).toBe(false);
  });

  it('should exclude object fields by default', () => {
    analyzer.analyzeDocuments(createNestedDocuments());

    const suggestions = analyzer.suggestPromotions(0.5);

    // Object fields like 'user.profile' should be excluded by default
    expect(suggestions.find((s) => s.path === 'user.profile')).toBeUndefined();
    // But leaf fields should be included
    expect(suggestions.find((s) => s.path === 'user.profile.name')).toBeDefined();
  });

  it('should include object fields when explicitly allowed', () => {
    analyzer.analyzeDocuments(createNestedDocuments());

    const suggestions = analyzer.suggestPromotions(0.5, { excludeObjects: false });

    expect(suggestions.find((s) => s.path === 'user')).toBeDefined();
  });

  it('should not suggest null-only fields', () => {
    analyzer.analyzeDocuments([
      { optional: null },
      { optional: null },
    ]);

    const suggestions = analyzer.suggestPromotions(0.5);

    expect(suggestions.find((s) => s.path === 'optional')).toBeUndefined();
  });

  it('should include reason in suggestions', () => {
    analyzer.analyzeDocuments(createSimpleDocuments());

    const suggestions = analyzer.suggestPromotions(0.5);
    const nameSuggestion = suggestions.find((s) => s.path === 'name');

    expect(nameSuggestion?.reason).toContain('name');
    expect(nameSuggestion?.reason).toContain('100');
    expect(nameSuggestion?.reason).toContain('string');
  });

  it('should mark array suggestions with isArray flag', () => {
    analyzer.analyzeDocuments(createArrayDocuments());

    const suggestions = analyzer.suggestPromotions(0.5, { excludeArrays: false });
    const tagsSuggestion = suggestions.find((s) => s.path === 'tags');

    expect(tagsSuggestion?.isArray).toBe(true);
  });
});

// ============================================================================
// Reset and Merge Tests
// ============================================================================

describe('FieldAnalyzer.reset', () => {
  it('should clear all accumulated data', () => {
    const analyzer = new FieldAnalyzer();
    analyzer.analyzeDocuments(createSimpleDocuments());

    expect(analyzer.getDocumentCount()).toBe(3);
    expect(analyzer.getFieldPaths()).not.toEqual([]);

    analyzer.reset();

    expect(analyzer.getDocumentCount()).toBe(0);
    expect(analyzer.getFieldPaths()).toEqual([]);
  });
});

describe('FieldAnalyzer.merge', () => {
  it('should merge statistics from another analyzer', () => {
    const analyzer1 = new FieldAnalyzer();
    analyzer1.analyzeDocuments([
      { name: 'Alice', age: 30 },
    ]);

    const analyzer2 = new FieldAnalyzer();
    analyzer2.analyzeDocuments([
      { name: 'Bob', age: 25 },
      { name: 'Charlie' },
    ]);

    analyzer1.merge(analyzer2);

    expect(analyzer1.getDocumentCount()).toBe(3);

    const nameStats = analyzer1.getFieldStat('name');
    expect(nameStats?.count).toBe(3);

    const ageStats = analyzer1.getFieldStat('age');
    expect(ageStats?.count).toBe(2);
  });

  it('should merge type counts correctly', () => {
    const analyzer1 = new FieldAnalyzer();
    analyzer1.analyzeDocuments([
      { value: 'string' },
      { value: 'another' },
    ]);

    const analyzer2 = new FieldAnalyzer();
    analyzer2.analyzeDocuments([
      { value: 123 },
    ]);

    analyzer1.merge(analyzer2);

    const stats = analyzer1.getFieldStat('value');
    expect(stats?.types.get('string')).toBe(2);
    expect(stats?.types.get('number')).toBe(1);
  });

  it('should merge sample values and deduplicate', () => {
    const analyzer1 = new FieldAnalyzer();
    analyzer1.analyzeDocuments([
      { status: 'active' },
    ]);

    const analyzer2 = new FieldAnalyzer();
    analyzer2.analyzeDocuments([
      { status: 'active' },
      { status: 'inactive' },
    ]);

    analyzer1.merge(analyzer2);

    const stats = analyzer1.getFieldStat('status');
    expect(stats?.sampleValues).toHaveLength(2);
    expect(stats?.sampleValues).toContain('active');
    expect(stats?.sampleValues).toContain('inactive');
  });

  it('should add new fields from merged analyzer', () => {
    const analyzer1 = new FieldAnalyzer();
    analyzer1.analyzeDocuments([
      { field1: 'value' },
    ]);

    const analyzer2 = new FieldAnalyzer();
    analyzer2.analyzeDocuments([
      { field2: 'value' },
    ]);

    analyzer1.merge(analyzer2);

    expect(analyzer1.getFieldPaths()).toContain('field1');
    expect(analyzer1.getFieldPaths()).toContain('field2');
  });
});

// ============================================================================
// Array Field Analysis Tests
// ============================================================================

describe('FieldAnalyzer array analysis', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should detect array fields', () => {
    analyzer.analyzeDocuments(createArrayDocuments());

    const tagsStats = analyzer.getFieldStat('tags');
    expect(tagsStats?.dominantType).toBe('array');
  });

  it('should analyze array element structure for objects', () => {
    analyzer.analyzeDocuments([
      {
        items: [
          { name: 'item1', price: 10 },
          { name: 'item2', price: 20 },
        ],
      },
      {
        items: [
          { name: 'item3', price: 30 },
        ],
      },
    ]);

    const paths = analyzer.getFieldPaths();
    expect(paths).toContain('items');
    expect(paths).toContain('items[].name');
    expect(paths).toContain('items[].price');

    const nameStats = analyzer.getFieldStat('items[].name');
    expect(nameStats?.dominantType).toBe('string');
  });
});

// ============================================================================
// Utility Function Tests
// ============================================================================

describe('createFieldAnalyzer', () => {
  it('should create analyzer with options', () => {
    const analyzer = createFieldAnalyzer({ maxDepth: 3 });

    expect(analyzer).toBeInstanceOf(FieldAnalyzer);
  });
});

describe('analyzeDocuments utility', () => {
  it('should analyze and return field stats', () => {
    const stats = analyzeDocuments(createSimpleDocuments());

    expect(stats.has('name')).toBe(true);
    expect(stats.has('age')).toBe(true);
    expect(stats.has('email')).toBe(true);
  });

  it('should pass options to analyzer', () => {
    const docs = createNestedDocuments();
    const stats = analyzeDocuments(docs, { maxDepth: 1 });

    expect(stats.has('user.profile')).toBe(true);
    expect(stats.has('user.profile.name')).toBe(false);
  });
});

describe('suggestFieldPromotions utility', () => {
  it('should return promotion suggestions', () => {
    const suggestions = suggestFieldPromotions(createSimpleDocuments(), 0.5);

    expect(suggestions).toHaveLength(3);
    expect(suggestions.every((s) => s.frequency >= 0.5)).toBe(true);
  });

  it('should accept combined options', () => {
    const suggestions = suggestFieldPromotions(
      createSimpleDocuments(),
      0.5,
      { maxSuggestions: 1, maxDepth: 5 }
    );

    expect(suggestions).toHaveLength(1);
  });
});

// ============================================================================
// Edge Cases Tests
// ============================================================================

describe('FieldAnalyzer edge cases', () => {
  let analyzer: FieldAnalyzer;

  beforeEach(() => {
    analyzer = new FieldAnalyzer();
  });

  it('should handle empty documents', () => {
    analyzer.analyzeDocuments([{}, {}, {}]);

    expect(analyzer.getDocumentCount()).toBe(3);
    expect(analyzer.getFieldPaths()).toEqual([]);
  });

  it('should handle empty document array', () => {
    analyzer.analyzeDocuments([]);

    expect(analyzer.getDocumentCount()).toBe(0);
    expect(analyzer.getFieldPaths()).toEqual([]);

    const suggestions = analyzer.suggestPromotions(0.5);
    expect(suggestions).toEqual([]);
  });

  it('should handle undefined values', () => {
    analyzer.analyzeDocuments([
      { field: undefined },
    ]);

    const stats = analyzer.getFieldStat('field');
    expect(stats?.dominantType).toBe('null');
  });

  it('should handle special characters in field names', () => {
    analyzer.analyzeDocuments([
      { 'field-with-dashes': 'value', field_with_underscores: 'value' },
    ]);

    const paths = analyzer.getFieldPaths();
    expect(paths).toContain('field-with-dashes');
    expect(paths).toContain('field_with_underscores');
  });

  it('should handle very large numbers', () => {
    analyzer.analyzeDocuments([
      { bigNum: Number.MAX_SAFE_INTEGER },
      { bigNum: Number.MIN_SAFE_INTEGER },
    ]);

    const stats = analyzer.getFieldStat('bigNum');
    expect(stats?.dominantType).toBe('number');
  });

  it('should handle empty strings', () => {
    analyzer.analyzeDocuments([
      { field: '' },
      { field: 'value' },
    ]);

    const stats = analyzer.getFieldStat('field');
    expect(stats?.dominantType).toBe('string');
    expect(stats?.sampleValues).toContain('');
  });

  it('should handle empty arrays', () => {
    analyzer.analyzeDocuments([
      { items: [] },
    ]);

    const stats = analyzer.getFieldStat('items');
    expect(stats?.dominantType).toBe('array');
  });

  it('should calculate frequency correctly when field count differs from type count', () => {
    // This tests the case where a field exists with null values
    analyzer.analyzeDocuments([
      { field: 'value1' },
      { field: null },
      { field: 'value2' },
    ]);

    const stats = analyzer.getFieldStat('field');
    expect(stats?.count).toBe(3);
    expect(stats?.frequency).toBe(1);
    expect(stats?.types.get('string')).toBe(2);
    expect(stats?.types.get('null')).toBe(1);
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('FieldAnalyzer integration', () => {
  it('should handle realistic document structure', () => {
    const documents = [
      {
        _id: new ObjectId(),
        createdAt: new Date(),
        user: {
          name: 'Alice',
          email: 'alice@example.com',
          profile: {
            age: 30,
            location: 'NYC',
          },
        },
        tags: ['premium', 'verified'],
        metadata: {
          source: 'web',
          version: 1,
        },
      },
      {
        _id: new ObjectId(),
        createdAt: new Date(),
        user: {
          name: 'Bob',
          email: 'bob@example.com',
          profile: {
            age: 25,
          },
        },
        tags: ['free'],
        settings: {
          notifications: true,
        },
      },
      {
        _id: new ObjectId(),
        createdAt: new Date(),
        user: {
          name: 'Charlie',
          email: 'charlie@example.com',
          profile: {
            age: 35,
            location: 'LA',
          },
        },
        tags: ['premium'],
        metadata: {
          source: 'mobile',
        },
      },
    ];

    const analyzer = new FieldAnalyzer();
    analyzer.analyzeDocuments(documents);

    // Check universal fields
    expect(analyzer.getFieldStat('_id')?.frequency).toBe(1);
    expect(analyzer.getFieldStat('createdAt')?.frequency).toBe(1);
    expect(analyzer.getFieldStat('user.name')?.frequency).toBe(1);
    expect(analyzer.getFieldStat('user.email')?.frequency).toBe(1);

    // Check partial fields
    expect(analyzer.getFieldStat('user.profile.location')?.frequency).toBeCloseTo(2 / 3);
    expect(analyzer.getFieldStat('metadata')?.frequency).toBeCloseTo(2 / 3);
    expect(analyzer.getFieldStat('settings')?.frequency).toBeCloseTo(1 / 3);

    // Check types
    expect(analyzer.getFieldStat('_id')?.dominantType).toBe('objectId');
    expect(analyzer.getFieldStat('createdAt')?.dominantType).toBe('date');
    expect(analyzer.getFieldStat('user.profile.age')?.dominantType).toBe('number');
    expect(analyzer.getFieldStat('tags')?.dominantType).toBe('array');

    // Get promotion suggestions for commonly occurring fields
    const suggestions = analyzer.suggestPromotions(0.9);

    // Universal scalar fields should be suggested
    const suggestedPaths = suggestions.map((s) => s.path);
    expect(suggestedPaths).toContain('user.name');
    expect(suggestedPaths).toContain('user.email');
    expect(suggestedPaths).toContain('user.profile.age');

    // Partial fields should not be in high-threshold suggestions
    expect(suggestedPaths).not.toContain('user.profile.location');
    expect(suggestedPaths).not.toContain('settings.notifications');
  });

  it('should work with incremental analysis and merging', () => {
    // Simulate distributed analysis
    const batch1 = [
      { id: 1, name: 'Item 1', category: 'A' },
      { id: 2, name: 'Item 2', category: 'B' },
    ];

    const batch2 = [
      { id: 3, name: 'Item 3', category: 'A' },
      { id: 4, name: 'Item 4' }, // Missing category
    ];

    const analyzer1 = new FieldAnalyzer();
    analyzer1.analyzeDocuments(batch1);

    const analyzer2 = new FieldAnalyzer();
    analyzer2.analyzeDocuments(batch2);

    // Merge results
    analyzer1.merge(analyzer2);

    expect(analyzer1.getDocumentCount()).toBe(4);
    expect(analyzer1.getFieldStat('category')?.frequency).toBe(0.75);

    // Suggest with 75% threshold - should include category
    const suggestions = analyzer1.suggestPromotions(0.7);
    expect(suggestions.some((s) => s.path === 'category')).toBe(true);

    // Suggest with 80% threshold - should not include category
    const stricterSuggestions = analyzer1.suggestPromotions(0.8);
    expect(stricterSuggestions.some((s) => s.path === 'category')).toBe(false);
  });
});
