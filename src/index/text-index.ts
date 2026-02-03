/**
 * Text Index
 *
 * MongoDB-compatible full-text search index implementation.
 * Provides text tokenization, case-insensitive matching, and relevance scoring.
 */

// ============================================================================
// Types
// ============================================================================

/** Text index entry mapping term to documents and their scores */
export interface TextIndexEntry {
  /** Document ID */
  docId: string;
  /** Term frequency in this document */
  termFrequency: number;
  /** Fields where the term was found */
  fields: string[];
}

/** Text index metadata */
export interface TextIndexMetadata {
  name: string;
  fields: string[];
  weights: { [field: string]: number };
  default_language: string;
  createdAt: string;
}

/** Serialized text index for persistence */
export interface SerializedTextIndex {
  metadata: TextIndexMetadata;
  /** Term -> document entries mapping */
  index: { [term: string]: TextIndexEntry[] };
  /** Document count for IDF calculation */
  documentCount: number;
  /** Document -> term count for normalization */
  documentTermCounts: { [docId: string]: number };
}

/** Text search result with score */
export interface TextSearchResult {
  docId: string;
  score: number;
}

/** Text search options */
export interface TextSearchOptions {
  /** Language for stemming (currently ignored, kept for API compatibility) */
  $language?: string;
  /** Case sensitive search (default: false) */
  $caseSensitive?: boolean;
  /** Diacritic sensitive search (default: false) */
  $diacriticSensitive?: boolean;
}

/** Internal parsed query representation */
interface ParsedQuery {
  terms: string[];
  phrases: string[][];
  negated: string[];
}

// ============================================================================
// Stop Words
// ============================================================================

/**
 * Common English stop words that are filtered out during indexing.
 * These are high-frequency words that typically don't carry meaning for search.
 */
const ENGLISH_STOP_WORDS = new Set([
  'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
  'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'or', 'that',
  'the', 'to', 'was', 'were', 'will', 'with', 'the', 'this', 'but',
  'they', 'have', 'had', 'what', 'when', 'where', 'who', 'which',
  'why', 'how', 'all', 'each', 'every', 'both', 'few', 'more', 'most',
  'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same',
  'so', 'than', 'too', 'very', 'just', 'can', 'should', 'now',
]);

// ============================================================================
// Text Index
// ============================================================================

/**
 * Text Index for full-text search
 *
 * Implements MongoDB-compatible text search with:
 * - Tokenization (splitting text into words)
 * - Case-insensitive matching
 * - Stop word filtering
 * - TF-IDF relevance scoring
 *
 * @example
 * ```typescript
 * const textIndex = new TextIndex('content_text', ['title', 'body'], {
 *   title: 10, // Higher weight for title matches
 *   body: 1
 * });
 *
 * // Index documents
 * textIndex.indexDocument('doc1', { title: 'Hello World', body: 'This is a test' });
 *
 * // Search
 * const results = textIndex.search('hello');
 * // Returns [{ docId: 'doc1', score: 0.8 }]
 * ```
 */
export class TextIndex {
  /** Index name */
  readonly name: string;

  /** Fields to index */
  readonly fields: string[];

  /** Field weights for scoring */
  readonly weights: { [field: string]: number };

  /** Default language */
  readonly default_language: string;

  /** Inverted index: term -> document entries */
  private index: Map<string, TextIndexEntry[]> = new Map();

  /** Total document count for IDF calculation */
  private documentCount: number = 0;

  /** Document -> total term count for normalization */
  private documentTermCounts: Map<string, number> = new Map();

  /** Set of indexed document IDs */
  private indexedDocs: Set<string> = new Set();

  constructor(
    name: string,
    fields: string[],
    weights: { [field: string]: number } = {},
    default_language: string = 'english'
  ) {
    this.name = name;
    this.fields = fields;
    this.default_language = default_language;

    // Set default weight of 1 for fields without explicit weight
    this.weights = {};
    for (const field of fields) {
      this.weights[field] = weights[field] ?? 1;
    }
  }

  // --------------------------------------------------------------------------
  // Tokenization
  // --------------------------------------------------------------------------

  /**
   * Tokenize text into searchable terms.
   *
   * Process:
   * 1. Convert to lowercase
   * 2. Split on non-word characters
   * 3. Filter out stop words
   * 4. Filter out short tokens (< 2 chars)
   *
   * @param text - Text to tokenize
   * @returns Array of tokens
   */
  tokenize(text: string): string[] {
    if (!text || typeof text !== 'string') {
      return [];
    }

    // Convert to lowercase and split on non-word characters
    const words = text.toLowerCase().split(/\W+/);

    // Filter out stop words and short tokens
    return words.filter(word =>
      word.length >= 2 && !ENGLISH_STOP_WORDS.has(word)
    );
  }

  /**
   * Tokenize text with case and diacritic sensitivity options.
   *
   * @param text - Text to tokenize
   * @param caseSensitive - If true, preserve case
   * @param diacriticSensitive - If true, preserve diacritics
   * @returns Array of tokens
   */
  private tokenizeWithOptions(
    text: string,
    caseSensitive: boolean,
    diacriticSensitive: boolean
  ): string[] {
    if (!text || typeof text !== 'string') {
      return [];
    }

    // Apply case normalization if not case sensitive
    let processedText = caseSensitive ? text : text.toLowerCase();

    // Apply diacritic normalization if not diacritic sensitive
    if (!diacriticSensitive) {
      processedText = this.removeDiacritics(processedText);
    }

    // Split on non-word characters
    const words = processedText.split(/\W+/);

    // Filter out stop words (always check lowercase version) and short tokens
    return words.filter(word =>
      word.length >= 2 && !ENGLISH_STOP_WORDS.has(word.toLowerCase())
    );
  }

  /**
   * Remove diacritics from a string.
   * Converts accented characters to their base form.
   *
   * @param text - Text to normalize
   * @returns Text with diacritics removed
   */
  private removeDiacritics(text: string): string {
    return text.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  }

  /**
   * Get nested value from document using dot notation.
   */
  private getNestedValue(doc: Record<string, unknown>, path: string): unknown {
    const parts = path.split('.');
    let value: unknown = doc;

    for (const part of parts) {
      if (value === null || value === undefined) {
        return undefined;
      }
      if (typeof value === 'object') {
        value = (value as Record<string, unknown>)[part];
      } else {
        return undefined;
      }
    }

    return value;
  }

  /**
   * Extract text from a field value, handling strings and arrays.
   */
  private extractText(value: unknown): string {
    if (typeof value === 'string') {
      return value;
    }

    if (Array.isArray(value)) {
      return value
        .filter(v => typeof v === 'string')
        .join(' ');
    }

    return '';
  }

  // --------------------------------------------------------------------------
  // Indexing Operations
  // --------------------------------------------------------------------------

  /**
   * Index a document.
   *
   * Extracts text from configured fields, tokenizes it,
   * and adds to the inverted index.
   *
   * @param docId - Document ID
   * @param doc - Document to index
   */
  indexDocument(docId: string, doc: Record<string, unknown>): void {
    // Track if document was already indexed
    if (this.indexedDocs.has(docId)) {
      // Remove existing entries first
      this.unindexDocument(docId);
    }

    // Track new document
    this.indexedDocs.add(docId);
    this.documentCount++;

    let totalTerms = 0;
    const termFieldMap = new Map<string, Set<string>>();

    // Process each field
    for (const field of this.fields) {
      const value = this.getNestedValue(doc, field);
      const text = this.extractText(value);
      const tokens = this.tokenize(text);

      for (const token of tokens) {
        totalTerms++;

        // Track which fields contain this term
        if (!termFieldMap.has(token)) {
          termFieldMap.set(token, new Set());
        }
        termFieldMap.get(token)!.add(field);
      }
    }

    // Store document term count for normalization
    this.documentTermCounts.set(docId, totalTerms);

    // Add entries to inverted index
    for (const [term, fields] of termFieldMap) {
      const fieldArray = Array.from(fields);

      // Calculate term frequency (count how many times term appears)
      let termFrequency = 0;
      for (const field of this.fields) {
        const value = this.getNestedValue(doc, field);
        const text = this.extractText(value);
        const tokens = this.tokenize(text);
        termFrequency += tokens.filter(t => t === term).length;
      }

      const entry: TextIndexEntry = {
        docId,
        termFrequency,
        fields: fieldArray,
      };

      if (!this.index.has(term)) {
        this.index.set(term, []);
      }
      this.index.get(term)!.push(entry);
    }
  }

  /**
   * Remove a document from the index.
   *
   * @param docId - Document ID to remove
   */
  unindexDocument(docId: string): void {
    if (!this.indexedDocs.has(docId)) {
      return;
    }

    this.indexedDocs.delete(docId);
    this.documentCount--;
    this.documentTermCounts.delete(docId);

    // Remove from all term entries
    for (const [term, entries] of this.index) {
      const filtered = entries.filter(e => e.docId !== docId);
      if (filtered.length === 0) {
        this.index.delete(term);
      } else {
        this.index.set(term, filtered);
      }
    }
  }

  /**
   * Check if a document is indexed.
   */
  hasDocument(docId: string): boolean {
    return this.indexedDocs.has(docId);
  }

  /**
   * Get the total number of indexed documents.
   */
  get size(): number {
    return this.documentCount;
  }

  /**
   * Check if the index is empty.
   */
  get isEmpty(): boolean {
    return this.documentCount === 0;
  }

  /**
   * Clear the index.
   */
  clear(): void {
    this.index.clear();
    this.documentCount = 0;
    this.documentTermCounts.clear();
    this.indexedDocs.clear();
  }

  // --------------------------------------------------------------------------
  // Search Operations
  // --------------------------------------------------------------------------

  /**
   * Search for documents matching a text query.
   *
   * Supports:
   * - Multiple search terms (OR logic - any term matches, like MongoDB)
   * - Phrase search with quotes (e.g., "hello world") - requires all words
   * - Negation with minus (e.g., -excluded)
   * - Case sensitive search with $caseSensitive option
   * - Diacritic sensitive search with $diacriticSensitive option
   *
   * @param query - Search query string
   * @param options - Search options
   * @returns Array of document IDs with relevance scores
   */
  search(query: string, options: TextSearchOptions = {}): TextSearchResult[] {
    if (!query || typeof query !== 'string') {
      return [];
    }

    const caseSensitive = options.$caseSensitive ?? false;
    const diacriticSensitive = options.$diacriticSensitive ?? false;

    // Parse query into terms, phrases, and negations
    const parsed = this.parseQuery(query, caseSensitive, diacriticSensitive);

    if (parsed.terms.length === 0 && parsed.phrases.length === 0) {
      return [];
    }

    // Find documents matching any positive term (OR logic) or all phrases (AND logic for phrases)
    const candidateDocs = this.findMatchingDocsOr(parsed.terms, parsed.phrases, caseSensitive, diacriticSensitive);

    // Filter out documents matching negated terms
    const filteredDocs = this.filterNegatedDocs(candidateDocs, parsed.negated, caseSensitive, diacriticSensitive);

    // Calculate TF-IDF scores
    const results: TextSearchResult[] = [];

    for (const docId of filteredDocs) {
      const score = this.calculateScore(docId, parsed.terms, parsed.phrases);
      if (score > 0) {
        results.push({ docId, score });
      }
    }

    // Sort by score descending
    results.sort((a, b) => b.score - a.score);

    return results;
  }

  /**
   * Parse a search query into terms, phrases, and negations.
   *
   * @param query - Search query string
   * @param caseSensitive - If true, preserve case in terms
   * @param diacriticSensitive - If true, preserve diacritics in terms
   */
  private parseQuery(
    query: string,
    caseSensitive: boolean = false,
    diacriticSensitive: boolean = false
  ): ParsedQuery {
    const terms: string[] = [];
    const phrases: string[][] = [];
    const negated: string[] = [];

    // Extract quoted phrases
    const phraseRegex = /"([^"]+)"/g;
    const processedQuery = query.replace(phraseRegex, (_, phrase) => {
      const tokens = this.tokenizeWithOptions(phrase, caseSensitive, diacriticSensitive);
      if (tokens.length > 0) {
        phrases.push(tokens);
      }
      return ''; // Remove phrase from query
    });

    // Process remaining terms
    const words = processedQuery.split(/\s+/);

    for (const word of words) {
      if (!word) continue;

      // Check for negation
      if (word.startsWith('-') && word.length > 1) {
        const negatedWord = caseSensitive ? word.slice(1) : word.slice(1).toLowerCase();
        const normalizedNegated = diacriticSensitive ? negatedWord : this.removeDiacritics(negatedWord);
        if (!ENGLISH_STOP_WORDS.has(normalizedNegated.toLowerCase()) && normalizedNegated.length >= 2) {
          negated.push(normalizedNegated);
        }
        continue;
      }

      // Tokenize the word
      const tokens = this.tokenizeWithOptions(word, caseSensitive, diacriticSensitive);
      terms.push(...tokens);
    }

    return { terms, phrases, negated };
  }

  /**
   * Find documents matching any positive term (OR logic) and all phrases (AND logic).
   * This matches MongoDB's text search behavior.
   *
   * @param terms - Search terms (OR logic between terms)
   * @param phrases - Quoted phrases (AND logic - doc must match all phrases)
   * @param caseSensitive - If true, match case exactly
   * @param diacriticSensitive - If true, match diacritics exactly
   */
  private findMatchingDocsOr(
    terms: string[],
    phrases: string[][],
    caseSensitive: boolean = false,
    diacriticSensitive: boolean = false
  ): Set<string> {
    const result = new Set<string>();

    // Find docs matching any term (OR logic)
    for (const term of terms) {
      const matchingDocs = this.findDocsMatchingTerm(term, caseSensitive, diacriticSensitive);
      for (const docId of matchingDocs) {
        result.add(docId);
      }
    }

    // If we have phrases, we need to intersect with phrase results
    if (phrases.length > 0) {
      const phraseCandidates = new Set<string>();

      // Start with docs from first phrase
      const firstPhraseMatches = this.findPhraseMatchesWithOptions(phrases[0]!, caseSensitive, diacriticSensitive);

      // If we have terms, intersect phrase results with term results
      if (terms.length > 0) {
        for (const docId of firstPhraseMatches) {
          if (result.has(docId)) {
            phraseCandidates.add(docId);
          }
        }
      } else {
        // No terms, just use phrase matches
        for (const docId of firstPhraseMatches) {
          phraseCandidates.add(docId);
        }
      }

      // Intersect with remaining phrases
      for (let i = 1; i < phrases.length; i++) {
        const phraseMatches = this.findPhraseMatchesWithOptions(phrases[i]!, caseSensitive, diacriticSensitive);
        const toRemove: string[] = [];
        for (const docId of phraseCandidates) {
          if (!phraseMatches.has(docId)) {
            toRemove.push(docId);
          }
        }
        for (const docId of toRemove) {
          phraseCandidates.delete(docId);
        }
      }

      return phraseCandidates;
    }

    return result;
  }

  /**
   * Find documents containing a specific term with sensitivity options.
   */
  private findDocsMatchingTerm(
    searchTerm: string,
    caseSensitive: boolean,
    diacriticSensitive: boolean
  ): Set<string> {
    const result = new Set<string>();

    if (caseSensitive || diacriticSensitive) {
      // Need to scan all index entries and compare with sensitivity options
      for (const [indexedTerm, entries] of this.index) {
        let normalizedIndexed = indexedTerm;
        let normalizedSearch = searchTerm;

        if (!caseSensitive) {
          normalizedIndexed = normalizedIndexed.toLowerCase();
          normalizedSearch = normalizedSearch.toLowerCase();
        }

        if (!diacriticSensitive) {
          normalizedIndexed = this.removeDiacritics(normalizedIndexed);
          normalizedSearch = this.removeDiacritics(normalizedSearch);
        }

        if (normalizedIndexed === normalizedSearch) {
          for (const entry of entries) {
            result.add(entry.docId);
          }
        }
      }
    } else {
      // Standard case-insensitive, diacritic-insensitive search
      // The index is already lowercase, so just look up directly
      const entries = this.index.get(searchTerm);
      if (entries) {
        for (const entry of entries) {
          result.add(entry.docId);
        }
      }
    }

    return result;
  }

  /**
   * Find phrase matches with sensitivity options.
   */
  private findPhraseMatchesWithOptions(
    phrase: string[],
    caseSensitive: boolean,
    diacriticSensitive: boolean
  ): Set<string> {
    if (phrase.length === 0) {
      return new Set();
    }

    // Start with docs containing first term
    let candidates = this.findDocsMatchingTerm(phrase[0]!, caseSensitive, diacriticSensitive);

    // Filter to docs containing all remaining terms in phrase
    for (let i = 1; i < phrase.length; i++) {
      const termMatches = this.findDocsMatchingTerm(phrase[i]!, caseSensitive, diacriticSensitive);
      const newCandidates = new Set<string>();
      for (const docId of candidates) {
        if (termMatches.has(docId)) {
          newCandidates.add(docId);
        }
      }
      candidates = newCandidates;
    }

    return candidates;
  }

  /**
   * Filter out documents matching negated terms.
   *
   * @param docs - Set of candidate document IDs
   * @param negated - List of negated terms
   * @param caseSensitive - If true, match case exactly
   * @param diacriticSensitive - If true, match diacritics exactly
   */
  private filterNegatedDocs(
    docs: Set<string>,
    negated: string[],
    caseSensitive: boolean = false,
    diacriticSensitive: boolean = false
  ): Set<string> {
    if (negated.length === 0) {
      return docs;
    }

    const result = new Set(docs);

    for (const term of negated) {
      const excludedDocs = this.findDocsMatchingTerm(term, caseSensitive, diacriticSensitive);
      for (const docId of excludedDocs) {
        result.delete(docId);
      }
    }

    return result;
  }

  /**
   * Calculate TF-IDF score for a document.
   *
   * TF-IDF = Term Frequency * Inverse Document Frequency
   * - TF: How often term appears in document (normalized by doc length)
   * - IDF: How rare the term is across all documents
   *
   * Also applies field weights.
   *
   * Uses smoothed IDF formula: log((N + 1) / (df + 1)) + 1
   * This ensures positive scores even when all documents contain the term.
   */
  private calculateScore(
    docId: string,
    terms: string[],
    phrases: string[][]
  ): number {
    let score = 0;
    const docTermCount = this.documentTermCounts.get(docId) || 1;

    // Score individual terms
    for (const term of terms) {
      const entries = this.index.get(term);
      if (!entries) continue;

      const entry = entries.find(e => e.docId === docId);
      if (!entry) continue;

      // TF: Normalized term frequency
      const tf = entry.termFrequency / docTermCount;

      // IDF: Smoothed inverse document frequency
      // Using (N + 1) / (df + 1) + 1 to ensure positive values
      const docsWithTerm = entries.length;
      const idf = Math.log((this.documentCount + 1) / (docsWithTerm + 1)) + 1;

      // Apply field weights
      let weightMultiplier = 0;
      for (const field of entry.fields) {
        weightMultiplier += this.weights[field] || 1;
      }
      weightMultiplier /= entry.fields.length; // Average weight

      score += tf * idf * weightMultiplier;
    }

    // Score phrases (give bonus for phrase matches)
    for (const phrase of phrases) {
      for (const term of phrase) {
        const entries = this.index.get(term);
        if (!entries) continue;

        const entry = entries.find(e => e.docId === docId);
        if (!entry) continue;

        const tf = entry.termFrequency / docTermCount;
        // Smoothed IDF for phrase terms too
        const idf = Math.log((this.documentCount + 1) / (entries.length + 1)) + 1;

        let weightMultiplier = 0;
        for (const field of entry.fields) {
          weightMultiplier += this.weights[field] || 1;
        }
        weightMultiplier /= entry.fields.length;

        // Phrase match bonus (1.5x)
        score += tf * idf * weightMultiplier * 1.5;
      }
    }

    return score;
  }

  /**
   * Get document IDs matching a $text query.
   * Returns docIds sorted by relevance score.
   */
  getMatchingDocIds(searchQuery: string): string[] {
    const results = this.search(searchQuery);
    return results.map(r => r.docId);
  }

  /**
   * Get scores for documents matching a query.
   * Used for $meta: "textScore" projection.
   */
  getScores(searchQuery: string): Map<string, number> {
    const results = this.search(searchQuery);
    const scores = new Map<string, number>();
    for (const result of results) {
      scores.set(result.docId, result.score);
    }
    return scores;
  }

  // --------------------------------------------------------------------------
  // Serialization
  // --------------------------------------------------------------------------

  /**
   * Serialize the index for persistence.
   */
  serialize(): SerializedTextIndex {
    const indexObj: { [term: string]: TextIndexEntry[] } = {};
    for (const [term, entries] of this.index) {
      indexObj[term] = entries;
    }

    const docTermCountsObj: { [docId: string]: number } = {};
    for (const [docId, count] of this.documentTermCounts) {
      docTermCountsObj[docId] = count;
    }

    return {
      metadata: {
        name: this.name,
        fields: this.fields,
        weights: this.weights,
        default_language: this.default_language,
        createdAt: new Date().toISOString(),
      },
      index: indexObj,
      documentCount: this.documentCount,
      documentTermCounts: docTermCountsObj,
    };
  }

  /**
   * Deserialize a text index from storage.
   */
  static deserialize(data: SerializedTextIndex): TextIndex {
    const textIndex = new TextIndex(
      data.metadata.name,
      data.metadata.fields,
      data.metadata.weights,
      data.metadata.default_language
    );

    // Restore index
    for (const [term, entries] of Object.entries(data.index)) {
      textIndex.index.set(term, entries);
    }

    // Restore document counts
    textIndex.documentCount = data.documentCount;
    for (const [docId, count] of Object.entries(data.documentTermCounts)) {
      textIndex.documentTermCounts.set(docId, count);
      textIndex.indexedDocs.add(docId);
    }

    return textIndex;
  }

  /**
   * Convert to JSON string.
   */
  toJSON(): string {
    return JSON.stringify(this.serialize());
  }

  /**
   * Create from JSON string.
   */
  static fromJSON(json: string): TextIndex {
    return TextIndex.deserialize(JSON.parse(json));
  }
}

// ============================================================================
// Exports
// ============================================================================

export default TextIndex;
