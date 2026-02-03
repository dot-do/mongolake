/**
 * Text Index Tests
 *
 * Comprehensive tests for the text index data structure covering:
 * - Tokenization (splitting, stop words, case normalization)
 * - Document indexing and unindexing
 * - Search operations (single term, multiple terms, phrases, negation)
 * - Relevance scoring (TF-IDF with field weights)
 * - Serialization and deserialization
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { TextIndex } from '../../../src/index/text-index.js';

// ============================================================================
// Tokenization Tests
// ============================================================================

describe('TextIndex - Tokenization', () => {
  let textIndex: TextIndex;

  beforeEach(() => {
    textIndex = new TextIndex('test_index', ['content']);
  });

  it('should tokenize simple text', () => {
    const tokens = textIndex.tokenize('Hello World');
    expect(tokens).toEqual(['hello', 'world']);
  });

  it('should convert to lowercase', () => {
    const tokens = textIndex.tokenize('HELLO WORLD');
    expect(tokens).toEqual(['hello', 'world']);
  });

  it('should split on non-word characters', () => {
    const tokens = textIndex.tokenize('hello-world, foo.bar!');
    expect(tokens).toEqual(['hello', 'world', 'foo', 'bar']);
  });

  it('should filter out stop words', () => {
    const tokens = textIndex.tokenize('the quick brown fox and the lazy dog');
    // 'the', 'and' are stop words
    expect(tokens).not.toContain('the');
    expect(tokens).not.toContain('and');
    expect(tokens).toContain('quick');
    expect(tokens).toContain('brown');
    expect(tokens).toContain('fox');
    expect(tokens).toContain('lazy');
    expect(tokens).toContain('dog');
  });

  it('should filter out short tokens (< 2 chars)', () => {
    const tokens = textIndex.tokenize('a b cc ddd');
    expect(tokens).not.toContain('a');
    expect(tokens).not.toContain('b');
    expect(tokens).toContain('cc');
    expect(tokens).toContain('ddd');
  });

  it('should handle empty string', () => {
    const tokens = textIndex.tokenize('');
    expect(tokens).toEqual([]);
  });

  it('should handle null/undefined', () => {
    expect(textIndex.tokenize(null as unknown as string)).toEqual([]);
    expect(textIndex.tokenize(undefined as unknown as string)).toEqual([]);
  });

  it('should handle only stop words', () => {
    const tokens = textIndex.tokenize('the and or is a an');
    expect(tokens).toEqual([]);
  });
});

// ============================================================================
// Document Indexing Tests
// ============================================================================

describe('TextIndex - Document Indexing', () => {
  let textIndex: TextIndex;

  beforeEach(() => {
    textIndex = new TextIndex('test_index', ['title', 'body']);
  });

  it('should index a simple document', () => {
    textIndex.indexDocument('doc1', { title: 'Hello World', body: 'Test content' });

    expect(textIndex.size).toBe(1);
    expect(textIndex.hasDocument('doc1')).toBe(true);
  });

  it('should index multiple documents', () => {
    textIndex.indexDocument('doc1', { title: 'Hello', body: 'World' });
    textIndex.indexDocument('doc2', { title: 'Foo', body: 'Bar' });
    textIndex.indexDocument('doc3', { title: 'Test', body: 'Document' });

    expect(textIndex.size).toBe(3);
    expect(textIndex.hasDocument('doc1')).toBe(true);
    expect(textIndex.hasDocument('doc2')).toBe(true);
    expect(textIndex.hasDocument('doc3')).toBe(true);
  });

  it('should handle re-indexing same document', () => {
    textIndex.indexDocument('doc1', { title: 'Hello', body: 'World' });
    textIndex.indexDocument('doc1', { title: 'Goodbye', body: 'World' });

    expect(textIndex.size).toBe(1);
    // Old content should not be searchable
    const helloResults = textIndex.search('hello');
    expect(helloResults.length).toBe(0);
    // New content should be searchable
    const goodbyeResults = textIndex.search('goodbye');
    expect(goodbyeResults.length).toBe(1);
  });

  it('should unindex a document', () => {
    textIndex.indexDocument('doc1', { title: 'Hello', body: 'World' });
    textIndex.indexDocument('doc2', { title: 'Foo', body: 'Bar' });

    textIndex.unindexDocument('doc1');

    expect(textIndex.size).toBe(1);
    expect(textIndex.hasDocument('doc1')).toBe(false);
    expect(textIndex.hasDocument('doc2')).toBe(true);
  });

  it('should handle unindexing non-existent document', () => {
    textIndex.unindexDocument('nonexistent');
    expect(textIndex.size).toBe(0);
  });

  it('should clear all documents', () => {
    textIndex.indexDocument('doc1', { title: 'Hello', body: 'World' });
    textIndex.indexDocument('doc2', { title: 'Foo', body: 'Bar' });

    textIndex.clear();

    expect(textIndex.isEmpty).toBe(true);
    expect(textIndex.size).toBe(0);
  });

  it('should handle nested fields', () => {
    const nestedIndex = new TextIndex('nested_index', ['nested.title']);
    nestedIndex.indexDocument('doc1', { nested: { title: 'Hello World' } });

    const results = nestedIndex.search('hello');
    expect(results.length).toBe(1);
    expect(results[0].docId).toBe('doc1');
  });

  it('should handle array fields', () => {
    const arrayIndex = new TextIndex('array_index', ['tags']);
    arrayIndex.indexDocument('doc1', { tags: ['javascript', 'typescript', 'nodejs'] });

    const results = arrayIndex.search('javascript');
    expect(results.length).toBe(1);
    expect(results[0].docId).toBe('doc1');
  });
});

// ============================================================================
// Search Tests
// ============================================================================

describe('TextIndex - Search', () => {
  let textIndex: TextIndex;

  beforeEach(() => {
    textIndex = new TextIndex('test_index', ['title', 'body']);
    textIndex.indexDocument('doc1', {
      title: 'Introduction to MongoDB',
      body: 'MongoDB is a document database.',
    });
    textIndex.indexDocument('doc2', {
      title: 'PostgreSQL Guide',
      body: 'PostgreSQL is a relational database.',
    });
    textIndex.indexDocument('doc3', {
      title: 'Database Comparison',
      body: 'Comparing MongoDB and PostgreSQL databases.',
    });
  });

  describe('single term search', () => {
    it('should find documents containing a term', () => {
      const results = textIndex.search('mongodb');
      expect(results.length).toBe(2);
      const docIds = results.map((r) => r.docId);
      expect(docIds).toContain('doc1');
      expect(docIds).toContain('doc3');
    });

    it('should return empty for non-matching term', () => {
      const results = textIndex.search('mysql');
      expect(results.length).toBe(0);
    });

    it('should be case insensitive', () => {
      const results = textIndex.search('MONGODB');
      expect(results.length).toBe(2);
    });

    it('should handle empty query', () => {
      const results = textIndex.search('');
      expect(results.length).toBe(0);
    });

    it('should handle only stop words in query', () => {
      const results = textIndex.search('the and or');
      expect(results.length).toBe(0);
    });
  });

  describe('multiple term search (OR logic)', () => {
    it('should find documents matching any term', () => {
      // MongoDB uses OR logic for space-separated terms
      const results = textIndex.search('mongodb postgresql');
      // Should find all docs containing either mongodb or postgresql
      expect(results.length).toBe(3);
      const docIds = results.map((r) => r.docId);
      expect(docIds).toContain('doc1'); // has mongodb
      expect(docIds).toContain('doc2'); // has postgresql
      expect(docIds).toContain('doc3'); // has both
    });

    it('should find documents even if only one term exists', () => {
      // mysql doesn't exist, but mongodb does
      const results = textIndex.search('mongodb mysql');
      expect(results.length).toBe(2);
      const docIds = results.map((r) => r.docId);
      expect(docIds).toContain('doc1');
      expect(docIds).toContain('doc3');
    });
  });

  describe('phrase search', () => {
    it('should find exact phrase', () => {
      const results = textIndex.search('"document database"');
      expect(results.length).toBe(1);
      expect(results[0].docId).toBe('doc1');
    });

    it('should not find partial phrase match', () => {
      const results = textIndex.search('"relational document"');
      expect(results.length).toBe(0);
    });
  });

  describe('negation search', () => {
    it('should exclude documents with negated term', () => {
      const results = textIndex.search('database -postgresql');
      expect(results.length).toBe(1);
      expect(results[0].docId).toBe('doc1');
    });

    it('should handle only negation (returns empty)', () => {
      const results = textIndex.search('-mongodb');
      expect(results.length).toBe(0);
    });
  });

  describe('combined search', () => {
    it('should combine terms, phrases, and negation', () => {
      const results = textIndex.search('database "mongodb" -relational');
      const docIds = results.map((r) => r.docId);
      expect(docIds).toContain('doc1');
      expect(docIds).toContain('doc3');
      expect(docIds).not.toContain('doc2');
    });
  });
});

// ============================================================================
// Relevance Scoring Tests
// ============================================================================

describe('TextIndex - Relevance Scoring', () => {
  it('should score documents by term frequency', () => {
    const textIndex = new TextIndex('test_index', ['body']);

    // doc1 mentions "database" once
    textIndex.indexDocument('doc1', { body: 'database systems' });
    // doc2 mentions "database" twice
    textIndex.indexDocument('doc2', { body: 'database database design' });

    const results = textIndex.search('database');
    expect(results.length).toBe(2);

    // doc2 should have higher score due to more occurrences
    const doc1Score = results.find((r) => r.docId === 'doc1')?.score || 0;
    const doc2Score = results.find((r) => r.docId === 'doc2')?.score || 0;
    expect(doc2Score).toBeGreaterThan(doc1Score);
  });

  it('should apply field weights', () => {
    const weightedIndex = new TextIndex('weighted', ['title', 'body'], {
      title: 10,
      body: 1,
    });

    // doc1: "mongodb" in title (high weight)
    weightedIndex.indexDocument('doc1', {
      title: 'mongodb guide',
      body: 'database tutorial',
    });
    // doc2: "mongodb" in body (low weight)
    weightedIndex.indexDocument('doc2', {
      title: 'database guide',
      body: 'mongodb tutorial',
    });

    const results = weightedIndex.search('mongodb');
    expect(results.length).toBe(2);

    // doc1 should have higher score due to title weight
    const doc1Score = results.find((r) => r.docId === 'doc1')?.score || 0;
    const doc2Score = results.find((r) => r.docId === 'doc2')?.score || 0;
    expect(doc1Score).toBeGreaterThan(doc2Score);
  });

  it('should return scores via getScores', () => {
    const textIndex = new TextIndex('test_index', ['body']);
    textIndex.indexDocument('doc1', { body: 'hello world' });
    textIndex.indexDocument('doc2', { body: 'hello there' });

    const scores = textIndex.getScores('hello');
    expect(scores.size).toBe(2);
    expect(scores.has('doc1')).toBe(true);
    expect(scores.has('doc2')).toBe(true);
  });

  it('should sort results by score descending', () => {
    const textIndex = new TextIndex('test_index', ['body']);
    textIndex.indexDocument('doc1', { body: 'test once' });
    textIndex.indexDocument('doc2', { body: 'test test test' });
    textIndex.indexDocument('doc3', { body: 'test test' });

    const results = textIndex.search('test');
    expect(results.length).toBe(3);

    // Should be sorted by score descending
    for (let i = 1; i < results.length; i++) {
      expect(results[i - 1].score).toBeGreaterThanOrEqual(results[i].score);
    }
  });
});

// ============================================================================
// Helper Methods Tests
// ============================================================================

describe('TextIndex - Helper Methods', () => {
  let textIndex: TextIndex;

  beforeEach(() => {
    textIndex = new TextIndex('test_index', ['body']);
    textIndex.indexDocument('doc1', { body: 'hello world' });
    textIndex.indexDocument('doc2', { body: 'hello there' });
    textIndex.indexDocument('doc3', { body: 'goodbye world' });
  });

  it('should return matching docIds', () => {
    const docIds = textIndex.getMatchingDocIds('hello');
    expect(docIds.length).toBe(2);
    expect(docIds).toContain('doc1');
    expect(docIds).toContain('doc2');
  });

  it('should return empty array for non-matching query', () => {
    const docIds = textIndex.getMatchingDocIds('nonexistent');
    expect(docIds.length).toBe(0);
  });
});

// ============================================================================
// Serialization Tests
// ============================================================================

describe('TextIndex - Serialization', () => {
  let textIndex: TextIndex;

  beforeEach(() => {
    textIndex = new TextIndex('test_index', ['title', 'body'], {
      title: 10,
      body: 1,
    }, 'english');
    textIndex.indexDocument('doc1', { title: 'Hello World', body: 'Test content' });
    textIndex.indexDocument('doc2', { title: 'Foo Bar', body: 'More content' });
  });

  it('should serialize to plain object', () => {
    const serialized = textIndex.serialize();

    expect(serialized.metadata.name).toBe('test_index');
    expect(serialized.metadata.fields).toEqual(['title', 'body']);
    expect(serialized.metadata.weights).toEqual({ title: 10, body: 1 });
    expect(serialized.metadata.default_language).toBe('english');
    expect(serialized.documentCount).toBe(2);
    expect(Object.keys(serialized.index).length).toBeGreaterThan(0);
  });

  it('should deserialize back to working index', () => {
    const serialized = textIndex.serialize();
    const restored = TextIndex.deserialize(serialized);

    expect(restored.name).toBe('test_index');
    expect(restored.fields).toEqual(['title', 'body']);
    expect(restored.size).toBe(2);

    // Should be able to search
    const results = restored.search('hello');
    expect(results.length).toBe(1);
    expect(results[0].docId).toBe('doc1');
  });

  it('should convert to JSON and back', () => {
    const json = textIndex.toJSON();
    const restored = TextIndex.fromJSON(json);

    expect(restored.size).toBe(2);
    expect(restored.search('foo').length).toBe(1);
  });

  it('should handle empty index serialization', () => {
    const emptyIndex = new TextIndex('empty', ['field']);
    const serialized = emptyIndex.serialize();

    expect(serialized.documentCount).toBe(0);
    expect(Object.keys(serialized.index).length).toBe(0);

    const restored = TextIndex.deserialize(serialized);
    expect(restored.isEmpty).toBe(true);
  });
});

// ============================================================================
// Edge Cases
// ============================================================================

describe('TextIndex - Edge Cases', () => {
  it('should handle documents with missing fields', () => {
    const textIndex = new TextIndex('test_index', ['title', 'body']);
    textIndex.indexDocument('doc1', { title: 'Hello' }); // missing body

    const results = textIndex.search('hello');
    expect(results.length).toBe(1);
  });

  it('should handle documents with null fields', () => {
    const textIndex = new TextIndex('test_index', ['title']);
    textIndex.indexDocument('doc1', { title: null });

    expect(textIndex.size).toBe(1);
    const results = textIndex.search('anything');
    expect(results.length).toBe(0);
  });

  it('should handle documents with non-string fields', () => {
    const textIndex = new TextIndex('test_index', ['value']);
    textIndex.indexDocument('doc1', { value: 123 });
    textIndex.indexDocument('doc2', { value: { nested: 'object' } });

    expect(textIndex.size).toBe(2);
  });

  it('should handle special characters in search', () => {
    const textIndex = new TextIndex('test_index', ['body']);
    textIndex.indexDocument('doc1', { body: 'hello@world.com' });

    const results = textIndex.search('hello');
    expect(results.length).toBe(1);
  });

  it('should handle unicode text', () => {
    const textIndex = new TextIndex('test_index', ['body']);
    textIndex.indexDocument('doc1', { body: 'cafe coffee' });

    const results = textIndex.search('cafe');
    expect(results.length).toBe(1);
  });

  it('should handle very long text', () => {
    const textIndex = new TextIndex('test_index', ['body']);
    const longText = 'word '.repeat(10000);
    textIndex.indexDocument('doc1', { body: longText });

    const results = textIndex.search('word');
    expect(results.length).toBe(1);
  });
});

// ============================================================================
// Index Properties
// ============================================================================

describe('TextIndex - Properties', () => {
  it('should expose name property', () => {
    const textIndex = new TextIndex('my_index', ['field']);
    expect(textIndex.name).toBe('my_index');
  });

  it('should expose fields property', () => {
    const textIndex = new TextIndex('test', ['title', 'body', 'tags']);
    expect(textIndex.fields).toEqual(['title', 'body', 'tags']);
  });

  it('should expose weights property', () => {
    const textIndex = new TextIndex('test', ['title', 'body'], {
      title: 5,
      body: 2,
    });
    expect(textIndex.weights).toEqual({ title: 5, body: 2 });
  });

  it('should set default weight of 1 for unspecified fields', () => {
    const textIndex = new TextIndex('test', ['title', 'body'], {
      title: 5, // body not specified
    });
    expect(textIndex.weights.title).toBe(5);
    expect(textIndex.weights.body).toBe(1);
  });

  it('should expose default_language property', () => {
    const textIndex = new TextIndex('test', ['field'], {}, 'spanish');
    expect(textIndex.default_language).toBe('spanish');
  });

  it('should default to english language', () => {
    const textIndex = new TextIndex('test', ['field']);
    expect(textIndex.default_language).toBe('english');
  });
});
