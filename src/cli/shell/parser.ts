/**
 * Shell Command Parser
 *
 * Parses MongoDB shell commands including:
 * - Built-in commands (exit, help, clear, use, show)
 * - Database method calls (db.collection.method)
 * - Method chaining (find().sort().limit())
 * - JavaScript-style object notation
 *
 * @module cli/shell/parser
 */

// ============================================================================
// Types
// ============================================================================

/**
 * Parsed command result
 */
export interface ParsedCommand {
  type: CommandType;
  args: unknown[];
}

/**
 * All supported command types
 */
export type CommandType =
  | 'exit'
  | 'help'
  | 'clear'
  | 'it'
  | 'editor'
  | 'showDbs'
  | 'showCollections'
  | 'useDb'
  | 'version'
  | 'print'
  | 'printjson'
  | 'load'
  | 'setBatchSize'
  | 'dbStats'
  | 'createCollection'
  | 'dropDatabase'
  | 'dbMethodChain'
  | 'error'
  | 'unknown';

/**
 * Parsed method call with its arguments
 */
export interface ParsedMethod {
  method: string;
  args: string;
}

// ============================================================================
// Bracket Balancing
// ============================================================================

/**
 * Check if input has balanced brackets (for multi-line detection).
 *
 * Handles:
 * - Curly braces {}
 * - Square brackets []
 * - Parentheses ()
 * - String escaping (ignores brackets inside strings)
 *
 * @param input - Input string to check
 * @returns true if all brackets are balanced
 */
export function hasBalancedBrackets(input: string): boolean {
  let braceCount = 0;
  let bracketCount = 0;
  let parenCount = 0;
  let inString = false;
  let stringChar = '';

  for (let i = 0; i < input.length; i++) {
    const char = input[i];
    const prevChar = i > 0 ? input[i - 1] : '';

    // Track string state
    if (inString) {
      if (char === stringChar && prevChar !== '\\') {
        inString = false;
      }
      continue;
    }

    // Enter string
    if (char === '"' || char === "'") {
      inString = true;
      stringChar = char;
      continue;
    }

    // Count brackets
    switch (char) {
      case '{': braceCount++; break;
      case '}': braceCount--; break;
      case '[': bracketCount++; break;
      case ']': bracketCount--; break;
      case '(': parenCount++; break;
      case ')': parenCount--; break;
    }
  }

  return braceCount === 0 && bracketCount === 0 && parenCount === 0;
}

// ============================================================================
// Command Parsing
// ============================================================================

/**
 * Parse a MongoDB shell command string.
 *
 * @param input - Raw command input
 * @returns Parsed command or null for empty input
 */
export function parseCommand(input: string): ParsedCommand | null {
  const trimmed = input.trim();

  if (!trimmed) {
    return null;
  }

  // Check for built-in commands first (most common)
  const builtIn = parseBuiltInCommand(trimmed);
  if (builtIn) {
    return builtIn;
  }

  // Check for function calls (print, printjson, load, version)
  const funcCall = parseFunctionCall(trimmed);
  if (funcCall) {
    return funcCall;
  }

  // Check for configuration commands
  const configCmd = parseConfigCommand(trimmed);
  if (configCmd) {
    return configCmd;
  }

  // Check for database commands (db.stats(), db.createCollection(), etc.)
  const dbCmd = parseDatabaseCommand(trimmed);
  if (dbCmd) {
    return dbCmd;
  }

  // Check for collection method chains (db.collection.method())
  const methodChain = parseDbMethodChain(trimmed);
  if (methodChain) {
    return methodChain;
  }

  // Unknown command
  return { type: 'unknown', args: [trimmed] };
}

/**
 * Parse built-in shell commands.
 */
function parseBuiltInCommand(input: string): ParsedCommand | null {
  // Exit commands
  if (input === 'exit' || input === 'quit') {
    return { type: 'exit', args: [] };
  }

  // Help command
  if (input === 'help') {
    return { type: 'help', args: [] };
  }

  // Clear command
  if (input === 'clear' || input === 'cls') {
    return { type: 'clear', args: [] };
  }

  // Iterate cursor command
  if (input === 'it') {
    return { type: 'it', args: [] };
  }

  // Editor mode
  if (input === '.editor') {
    return { type: 'editor', args: [] };
  }

  // Show databases
  if (input === 'show dbs' || input === 'show databases') {
    return { type: 'showDbs', args: [] };
  }

  // Show collections
  if (input === 'show collections' || input === 'show tables') {
    return { type: 'showCollections', args: [] };
  }

  // Use database
  const useMatch = input.match(/^use\s+(\w+)$/);
  if (useMatch) {
    return { type: 'useDb', args: [useMatch[1]] };
  }

  return null;
}

/**
 * Parse function call commands (print, printjson, load, version).
 */
function parseFunctionCall(input: string): ParsedCommand | null {
  // version()
  if (input === 'version()') {
    return { type: 'version', args: [] };
  }

  // print()
  const printMatch = input.match(/^print\((.+)\)$/);
  if (printMatch) {
    return { type: 'print', args: [printMatch[1]] };
  }

  // printjson()
  const printjsonMatch = input.match(/^printjson\((.+)\)$/);
  if (printjsonMatch) {
    return { type: 'printjson', args: [printjsonMatch[1]] };
  }

  // load()
  const loadMatch = input.match(/^load\(["'](.+)["']\)$/);
  if (loadMatch) {
    return { type: 'load', args: [loadMatch[1]] };
  }

  return null;
}

/**
 * Parse configuration commands.
 */
function parseConfigCommand(input: string): ParsedCommand | null {
  // DBQuery.shellBatchSize = N
  const batchSizeMatch = input.match(/^DBQuery\.shellBatchSize\s*=\s*(\d+)$/);
  if (batchSizeMatch) {
    return { type: 'setBatchSize', args: [parseInt(batchSizeMatch[1]!, 10)] };
  }

  return null;
}

/**
 * Parse database-level commands.
 */
function parseDatabaseCommand(input: string): ParsedCommand | null {
  // db.stats()
  if (input === 'db.stats()') {
    return { type: 'dbStats', args: [] };
  }

  // db.createCollection("name")
  const createCollMatch = input.match(/^db\.createCollection\(["'](\w+)["']\)$/);
  if (createCollMatch) {
    return { type: 'createCollection', args: [createCollMatch[1]] };
  }

  // db.dropDatabase()
  if (input === 'db.dropDatabase()') {
    return { type: 'dropDatabase', args: [] };
  }

  // db.getCollectionNames()
  if (input === 'db.getCollectionNames()') {
    return { type: 'showCollections', args: [] };
  }

  return null;
}

/**
 * Parse db.collection.method() chains.
 */
function parseDbMethodChain(input: string): ParsedCommand | null {
  const dbMethodMatch = input.match(/^db\.(\w+)\.(.+)$/);
  if (dbMethodMatch) {
    const [, collection, methodChain] = dbMethodMatch;
    return { type: 'dbMethodChain', args: [collection, methodChain] };
  }
  return null;
}

// ============================================================================
// Method Chain Parsing
// ============================================================================

/**
 * Parse a method chain like: find({}).sort({}).limit(5)
 *
 * Handles nested brackets correctly by tracking depth.
 *
 * @param methodChain - Method chain string
 * @returns Array of parsed methods
 */
export function parseMethodChain(methodChain: string): ParsedMethod[] {
  const methods: ParsedMethod[] = [];
  let remaining = methodChain;

  while (remaining) {
    // Match method name
    const methodMatch = remaining.match(/^(\w+)\(/);
    if (!methodMatch) break;

    const method = methodMatch[1];
    const argsStart = methodMatch[0].length;

    // Find matching closing paren
    const argsEnd = findMatchingParen(remaining, argsStart);
    if (argsEnd === -1) break;

    const args = remaining.substring(argsStart, argsEnd).trim();
    methods.push({ method: method!, args });

    // Check for chained method (starts with .)
    remaining = remaining.substring(argsEnd + 1).trim();
    if (remaining.startsWith('.')) {
      remaining = remaining.slice(1);
    } else if (remaining) {
      break;
    }
  }

  return methods;
}

/**
 * Find the matching closing parenthesis.
 *
 * @param str - String to search
 * @param start - Position after opening paren
 * @returns Position of closing paren, or -1 if not found
 */
function findMatchingParen(str: string, start: number): number {
  let depth = 1;
  let inString = false;
  let stringChar = '';

  for (let i = start; i < str.length && depth > 0; i++) {
    const char = str[i];
    const prevChar = i > 0 ? str[i - 1] : '';

    if (inString) {
      if (char === stringChar && prevChar !== '\\') {
        inString = false;
      }
      continue;
    }

    if (char === '"' || char === "'") {
      inString = true;
      stringChar = char;
    } else if (char === '(' || char === '{' || char === '[') {
      depth++;
    } else if (char === ')' || char === '}' || char === ']') {
      depth--;
      if (depth === 0) {
        return i;
      }
    }
  }

  return -1;
}

// ============================================================================
// Argument Parsing
// ============================================================================

/**
 * Convert JavaScript-style object notation to JSON.
 *
 * Handles unquoted keys: { name: 1 } -> { "name": 1 }
 * Handles $ prefixed keys: { $gt: 5 } -> { "$gt": 5 }
 *
 * @param input - JavaScript-style object string
 * @returns JSON-compatible string
 */
export function jsToJson(input: string): string {
  return input.replace(/([{,\[\s])(\$?\w+)\s*:/g, '$1"$2":');
}

/**
 * Parse an arguments string to an array of values.
 *
 * Tries multiple parsing strategies:
 * 1. Standard JSON
 * 2. JavaScript-to-JSON conversion
 * 3. Single expression parsing
 *
 * @param argsStr - Arguments string
 * @returns Array of parsed values
 * @throws Error if parsing fails
 */
export function parseArgs(argsStr: string): unknown[] {
  if (!argsStr.trim()) return [];

  // Try standard JSON first
  try {
    return JSON.parse(`[${argsStr}]`);
  } catch {
    // Continue to next strategy
  }

  // Try with JavaScript-to-JSON conversion
  try {
    const jsonStr = jsToJson(argsStr);
    return JSON.parse(`[${jsonStr}]`);
  } catch {
    // Continue to next strategy
  }

  // Try parsing as a single expression
  try {
    return [JSON.parse(argsStr)];
  } catch {
    // Continue to next strategy
  }

  // Try single expression with JS-to-JSON conversion
  try {
    const jsonStr = jsToJson(argsStr);
    return [JSON.parse(jsonStr)];
  } catch {
    throw new Error(`Invalid arguments: ${argsStr}`);
  }
}
