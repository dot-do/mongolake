/**
 * Shell Output Formatting
 *
 * Handles MongoDB-style output formatting including:
 * - Document pretty printing
 * - ISODate, ObjectId, BinData formatting
 * - Syntax highlighting for JSON output
 * - Output truncation for large documents
 *
 * @module cli/shell/output
 */

import {
  shouldUseColors,
  getExtendedColors,
} from '../utils.js';

// ============================================================================
// Constants
// ============================================================================

/** Maximum total output length before truncation */
export const MAX_OUTPUT_LENGTH = 4000;

/** Maximum string value length before truncation */
export const MAX_STRING_LENGTH = 500;

// ============================================================================
// Value Formatting
// ============================================================================

/**
 * Format a single value for MongoDB-style output.
 *
 * Handles special types:
 * - Date -> ISODate("...")
 * - ObjectId-like -> ObjectId("...")
 * - Binary data -> BinData(subtype, "base64")
 * - Arrays and objects -> Pretty printed with indentation
 *
 * @param value - The value to format
 * @param depth - Current indentation depth
 * @returns Formatted string representation
 */
export function formatValue(value: unknown, depth = 0): string {
  if (value === null) return 'null';
  if (value === undefined) return 'undefined';

  // Handle Date
  if (value instanceof Date) {
    return `ISODate("${value.toISOString()}")`;
  }

  // Handle ObjectId-like objects and Binary data
  if (typeof value === 'object' && value !== null) {
    const formatted = formatSpecialObject(value as Record<string, unknown>);
    if (formatted !== null) {
      return formatted;
    }
  }

  // Handle arrays
  if (Array.isArray(value)) {
    return formatArray(value, depth);
  }

  // Handle objects
  if (typeof value === 'object' && value !== null) {
    return formatObject(value as Record<string, unknown>, depth);
  }

  // Handle strings with truncation
  if (typeof value === 'string') {
    return formatString(value);
  }

  // Handle primitives (numbers, booleans)
  return JSON.stringify(value);
}

/**
 * Format special MongoDB objects (ObjectId, Binary, etc.)
 *
 * @param obj - Object to check and format
 * @returns Formatted string or null if not a special object
 */
function formatSpecialObject(obj: Record<string, unknown>): string | null {
  // ObjectId detection by constructor name
  if (typeof obj.toString === 'function' && obj.constructor?.name === 'ObjectId') {
    return `ObjectId("${obj.toString()}")`;
  }

  // ObjectId detection by _bsontype or hex string pattern
  if (obj._bsontype === 'ObjectId' ||
      (typeof obj.toString === 'function' && /^[0-9a-f]{24}$/.test(obj.toString()))) {
    return `ObjectId("${obj.toString()}")`;
  }

  // Binary data detection
  if (obj.buffer instanceof Buffer || (obj.sub_type !== undefined && obj.buffer)) {
    const subType = (obj.sub_type as number) || 0;
    const buffer = obj.buffer as Buffer;
    const base64 = buffer.toString('base64');
    return `BinData(${subType}, "${base64}")`;
  }

  // Plain Buffer
  if (Buffer.isBuffer(obj)) {
    return `BinData(0, "${(obj as Buffer).toString('base64')}")`;
  }

  return null;
}

/**
 * Format an array with proper indentation.
 *
 * @param arr - Array to format
 * @param depth - Current indentation depth
 * @returns Formatted array string
 */
function formatArray(arr: unknown[], depth: number): string {
  if (arr.length === 0) return '[]';

  const indent = '  '.repeat(depth + 1);
  const closingIndent = '  '.repeat(depth);
  const items = arr.map(item => formatValue(item, depth + 1));

  return `[\n${items.map(item => indent + item).join(',\n')}\n${closingIndent}]`;
}

/**
 * Format an object with proper indentation.
 *
 * @param obj - Object to format
 * @param depth - Current indentation depth
 * @returns Formatted object string
 */
function formatObject(obj: Record<string, unknown>, depth: number): string {
  const keys = Object.keys(obj);
  if (keys.length === 0) return '{}';

  const indent = '  '.repeat(depth + 1);
  const closingIndent = '  '.repeat(depth);

  const entries = keys.map(key => {
    const formattedValue = formatValue(obj[key], depth + 1);
    return `${indent}"${key}": ${formattedValue}`;
  });

  return `{\n${entries.join(',\n')}\n${closingIndent}}`;
}

/**
 * Format a string value with truncation.
 *
 * @param value - String to format
 * @returns JSON-escaped string, possibly truncated
 */
function formatString(value: string): string {
  const truncated = value.length > MAX_STRING_LENGTH
    ? value.substring(0, MAX_STRING_LENGTH) + '...'
    : value;
  return JSON.stringify(truncated);
}

// ============================================================================
// Output Formatting
// ============================================================================

/**
 * Format a value for display, with length truncation.
 *
 * @param value - Value to format
 * @returns Formatted string, truncated if too long
 */
export function formatOutput(value: unknown): string {
  const formatted = formatValue(value);
  if (formatted.length > MAX_OUTPUT_LENGTH) {
    return formatted.substring(0, MAX_OUTPUT_LENGTH) + '\n...';
  }
  return formatted;
}

// ============================================================================
// Syntax Highlighting
// ============================================================================

/**
 * Apply syntax highlighting to formatted JSON output.
 *
 * Colorizes:
 * - ISODate, ObjectId, BinData constructs (magenta)
 * - String values (green)
 * - Numbers (yellow)
 * - Booleans and null (magenta)
 *
 * @param output - Formatted output string
 * @returns Highlighted output string (or original if colors disabled)
 */
export function highlightOutput(output: string): string {
  if (!shouldUseColors()) return output;

  const colors = getExtendedColors();
  let result = output;

  // Placeholder system to protect already-colored regions
  const placeholders: string[] = [];
  const createPlaceholder = (s: string): string => {
    const idx = placeholders.length;
    placeholders.push(s);
    return `\x00PLACEHOLDER${idx}\x00`;
  };

  // Color MongoDB special types (ISODate, ObjectId, BinData) as a unit
  result = result.replace(
    /(ISODate|ObjectId|BinData)\([^)]+\)/g,
    match => createPlaceholder(`${colors.magenta}${match}${colors.reset}`)
  );

  // Color remaining strings (not in placeholders)
  result = result.replace(
    /"([^"\\]|\\.)*"/g,
    match => `${colors.green}${match}${colors.reset}`
  );

  // Protect colored strings before number pass
  result = result.replace(
    /\x1b\[[0-9;]*m[^\x00]*?\x1b\[0m/g,
    createPlaceholder
  );

  // Color numbers (outside protected regions)
  result = result.replace(
    /\b(-?\d+\.?\d*)\b/g,
    match => `${colors.yellow}${match}${colors.reset}`
  );

  // Color booleans and null
  result = result.replace(
    /\b(true|false|null)\b/g,
    match => `${colors.magenta}${match}${colors.reset}`
  );

  // Restore placeholders
  result = result.replace(
    /\x00PLACEHOLDER(\d+)\x00/g,
    (_, idx) => placeholders[parseInt(idx, 10)]!
  );

  return result;
}

// ============================================================================
// Message Printing
// ============================================================================

/**
 * Print a success message in green.
 */
export function printSuccess(message: string): void {
  const colors = getExtendedColors();
  console.log(`${colors.green}${message}${colors.reset}`);
}

/**
 * Print an error message in red with "Error:" prefix.
 */
export function printError(message: string): void {
  const colors = getExtendedColors();
  console.log(`${colors.red}Error: ${message}${colors.reset}`);
}

/**
 * Print an info message in cyan.
 */
export function printInfo(message: string): void {
  const colors = getExtendedColors();
  console.log(`${colors.cyan}${message}${colors.reset}`);
}
