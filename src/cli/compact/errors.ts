/**
 * Error handling for the Compact command
 *
 * @module cli/compact/errors
 */

/**
 * Custom error class for compaction-related errors
 */
export class CompactionError extends Error {
  code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = 'CompactionError';
    this.code = code;
  }
}

/**
 * Logger for compaction operations with severity levels
 */
export class CompactionLogger {
  error(code: string, message: string): void {
    console.error(`[ERROR] ${code}: ${message}`);
  }

  warn(code: string, message: string): void {
    console.warn(`[WARN] ${code}: ${message}`);
  }

  info(message: string): void {
    console.log(`[INFO] ${message}`);
  }

  debug(message: string): void {
    console.log(`[DEBUG] ${message}`);
  }
}

/**
 * Format an error message with actionable suggestions
 */
export function formatErrorMessage(error: CompactionError): string {
  let suggestion = '';

  switch (error.code) {
    case 'STORAGE_WRITE_ERROR':
      suggestion = 'suggestion: check disk space and permissions';
      break;
    case 'STORAGE_READ_ERROR':
      suggestion = 'suggestion: verify file exists and is readable';
      break;
    default:
      suggestion = 'suggestion: check logs for details';
  }

  return `${error.code}: ${error.message}\n${suggestion}`;
}
