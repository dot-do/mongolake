/**
 * Shell Tab Completion
 *
 * Provides tab completion for:
 * - Collection names (db.<partial>)
 * - Method names (db.collection.<partial>)
 * - Cursor methods (db.collection.find().<partial>)
 *
 * @module cli/shell/completer
 */

// ============================================================================
// Constants
// ============================================================================

/**
 * Available collection methods for completion
 */
const COLLECTION_METHODS = [
  'find',
  'findOne',
  'insertOne',
  'insertMany',
  'updateOne',
  'updateMany',
  'replaceOne',
  'deleteOne',
  'deleteMany',
  'countDocuments',
  'aggregate',
  'distinct',
  'drop',
  'createIndex',
  'getIndexes',
  'dropIndex',
  'stats',
  'bulkWrite',
];

/**
 * Available cursor methods for completion
 */
const CURSOR_METHODS = [
  'limit',
  'skip',
  'sort',
  'project',
  'explain',
  'toArray',
];

// ============================================================================
// Types
// ============================================================================

/**
 * Completer function signature for readline
 */
export type CompleterFunction = (line: string) => [string[], string];

/**
 * State needed for completion
 */
export interface CompletionState {
  collections: string[];
}

// ============================================================================
// Completer Factory
// ============================================================================

/**
 * Create a tab completer function for the shell.
 *
 * @param state - State containing available collection names
 * @returns Completer function for readline
 */
export function createCompleter(state: CompletionState): CompleterFunction {
  return (line: string): [string[], string] => {
    // Collection name completion: db.<partial>
    const collectionCompletion = completeCollectionName(line, state.collections);
    if (collectionCompletion) {
      return collectionCompletion;
    }

    // Method name completion: db.collection.<partial>
    const methodCompletion = completeMethodName(line);
    if (methodCompletion) {
      return methodCompletion;
    }

    // Cursor method completion: db.collection.find().<partial>
    const cursorCompletion = completeCursorMethod(line);
    if (cursorCompletion) {
      return cursorCompletion;
    }

    // No completion available
    return [[], line];
  };
}

// ============================================================================
// Completion Strategies
// ============================================================================

/**
 * Complete collection names after "db."
 */
function completeCollectionName(
  line: string,
  collections: string[]
): [string[], string] | null {
  const match = line.match(/^db\.(\w*)$/);
  if (!match) return null;

  const partial = match[1]!;
  const completions = collections
    .filter(c => c.startsWith(partial))
    .map(c => `db.${c}`);

  return [completions, line];
}

/**
 * Complete method names after "db.collection."
 */
function completeMethodName(line: string): [string[], string] | null {
  const match = line.match(/^db\.(\w+)\.(\w*)$/);
  if (!match) return null;

  const [, collection, partial] = match as [string, string, string];
  const completions = COLLECTION_METHODS
    .filter(m => m.startsWith(partial))
    .map(m => `db.${collection}.${m}`);

  return [completions, line];
}

/**
 * Complete cursor methods after "db.collection.find()."
 */
function completeCursorMethod(line: string): [string[], string] | null {
  const match = line.match(/^(db\.\w+\.find\([^)]*\)\.)(\w*)$/);
  if (!match) return null;

  const [, prefix, partial] = match as [string, string, string];
  const completions = CURSOR_METHODS
    .filter(m => m.startsWith(partial))
    .map(m => `${prefix}${m}`);

  return [completions, line];
}
