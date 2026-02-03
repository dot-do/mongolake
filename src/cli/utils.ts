/**
 * CLI Shared Utilities
 *
 * Common utilities shared across MongoLake CLI commands:
 * - ANSI color codes for terminal output
 * - Formatting functions for bytes, duration, etc.
 * - Progress bar display
 *
 * @module cli/utils
 */

// ============================================================================
// ANSI Color Codes
// ============================================================================

/**
 * Core ANSI color codes used across CLI commands.
 * These are the commonly used subset of colors.
 */
export interface CoreColors {
  reset: string;
  bright: string;
  dim: string;
  red: string;
  green: string;
  yellow: string;
  blue: string;
  cyan: string;
}

/**
 * Extended ANSI color codes including background colors.
 * Used by shell and other commands that need richer formatting.
 */
export interface ExtendedColors extends CoreColors {
  black: string;
  magenta: string;
  white: string;
  bgBlack: string;
  bgRed: string;
  bgGreen: string;
  bgYellow: string;
  bgBlue: string;
  bgMagenta: string;
  bgCyan: string;
  bgWhite: string;
}

/**
 * Core ANSI color codes for terminal output.
 * Minimal set used by most CLI commands.
 */
export const colors: CoreColors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
};

/**
 * Empty color codes for non-TTY output.
 */
export const noColors: CoreColors = {
  reset: '',
  bright: '',
  dim: '',
  red: '',
  green: '',
  yellow: '',
  blue: '',
  cyan: '',
};

/**
 * Extended color codes including background colors.
 */
export const extendedColors: ExtendedColors = {
  ...colors,
  black: '\x1b[30m',
  magenta: '\x1b[35m',
  white: '\x1b[37m',
  bgBlack: '\x1b[40m',
  bgRed: '\x1b[41m',
  bgGreen: '\x1b[42m',
  bgYellow: '\x1b[43m',
  bgBlue: '\x1b[44m',
  bgMagenta: '\x1b[45m',
  bgCyan: '\x1b[46m',
  bgWhite: '\x1b[47m',
};

/**
 * Empty extended color codes for non-TTY output.
 */
export const noExtendedColors: ExtendedColors = {
  ...noColors,
  black: '',
  magenta: '',
  white: '',
  bgBlack: '',
  bgRed: '',
  bgGreen: '',
  bgYellow: '',
  bgBlue: '',
  bgMagenta: '',
  bgCyan: '',
  bgWhite: '',
};

/**
 * Check if colors should be used (TTY detection).
 */
export function shouldUseColors(): boolean {
  return process.stdout.isTTY !== false;
}

/**
 * Get core color codes based on TTY detection.
 */
export function getColors(): CoreColors {
  return shouldUseColors() ? colors : noColors;
}

/**
 * Get extended color codes based on TTY detection.
 */
export function getExtendedColors(): ExtendedColors {
  return shouldUseColors() ? extendedColors : noExtendedColors;
}

// ============================================================================
// Formatting Utilities
// ============================================================================

/**
 * Format bytes to human-readable string.
 *
 * @param bytes - Number of bytes
 * @returns Formatted string (e.g., "1.50 MB")
 */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';

  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const k = 1024;
  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return `${(bytes / Math.pow(k, i)).toFixed(2)} ${units[i]}`;
}

/**
 * Format duration in milliseconds to human-readable string.
 *
 * @param ms - Duration in milliseconds
 * @returns Formatted string (e.g., "2m 30s")
 */
export function formatDuration(ms: number): string {
  if (ms < 1000) {
    return `${Math.round(ms)}ms`;
  }

  const seconds = ms / 1000;
  if (seconds < 60) {
    return `${seconds.toFixed(1)}s`;
  }

  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = Math.floor(seconds % 60);
  return `${minutes}m ${remainingSeconds}s`;
}

/**
 * Format a number with thousands separators.
 *
 * @param num - Number to format
 * @returns Formatted string (e.g., "1,234,567")
 */
export function formatNumber(num: number): string {
  return num.toLocaleString();
}

/**
 * Format a percentage.
 *
 * @param value - Value between 0 and 1
 * @param decimals - Number of decimal places (default: 1)
 * @returns Formatted string (e.g., "75.5%")
 */
export function formatPercent(value: number, decimals: number = 1): string {
  return `${(value * 100).toFixed(decimals)}%`;
}

// ============================================================================
// Progress Display
// ============================================================================

/**
 * Options for creating a progress bar.
 */
export interface ProgressBarOptions {
  /** Total number of items */
  total: number;
  /** Width of the progress bar in characters (default: 40) */
  width?: number;
  /** Show ETA calculation (default: true) */
  showEta?: boolean;
}

/**
 * A terminal progress bar for displaying long-running operations.
 */
export class ProgressBar {
  private current: number = 0;
  private total: number;
  private width: number;
  private startTime: number;
  private lastUpdate: number = 0;
  private showEta: boolean;

  constructor(options: ProgressBarOptions | number, width?: number) {
    // Support both object and legacy (total, width) signatures
    if (typeof options === 'number') {
      this.total = options;
      this.width = width ?? 40;
      this.showEta = true;
    } else {
      this.total = options.total;
      this.width = options.width ?? 40;
      this.showEta = options.showEta ?? true;
    }
    this.startTime = Date.now();
  }

  /**
   * Update the progress bar.
   *
   * @param current - Current progress count
   * @param label - Optional label to display
   */
  update(current: number, label?: string): void {
    this.current = current;
    const now = Date.now();

    // Throttle updates to max 10 per second
    if (now - this.lastUpdate < 100 && current < this.total) {
      return;
    }
    this.lastUpdate = now;

    const progress = this.total > 0 ? this.current / this.total : 0;
    const filled = Math.round(this.width * progress);
    const empty = this.width - filled;

    const bar = '[' + '='.repeat(filled) + ' '.repeat(empty) + ']';
    const percent = (progress * 100).toFixed(1).padStart(5);
    const count = `${this.current}/${this.total}`;

    // Calculate ETA
    let eta = '';
    if (this.showEta && progress > 0 && progress < 1) {
      const elapsed = (now - this.startTime) / 1000;
      const remaining = (elapsed / progress) * (1 - progress);
      eta = ` ETA: ${formatDuration(remaining * 1000)}`;
    }

    process.stdout.write(`\r${bar} ${percent}% ${count}${label ? ` - ${label}` : ''}${eta}     `);

    if (current >= this.total) {
      console.log('');
    }
  }

  /**
   * Increment progress by one.
   *
   * @param label - Optional label to display
   */
  increment(label?: string): void {
    this.update(this.current + 1, label);
  }

  /**
   * Complete the progress bar.
   */
  finish(): void {
    if (this.current < this.total) {
      this.update(this.total);
    }
  }

  /**
   * Get elapsed time in milliseconds.
   */
  getElapsed(): number {
    return Date.now() - this.startTime;
  }
}

// ============================================================================
// Console Output Helpers
// ============================================================================

/**
 * Print a success message in green.
 */
export function printSuccess(message: string): void {
  const c = getColors();
  console.log(`${c.green}${message}${c.reset}`);
}

/**
 * Print an error message in red.
 */
export function printError(message: string): void {
  const c = getColors();
  console.error(`${c.red}${message}${c.reset}`);
}

/**
 * Print a warning message in yellow.
 */
export function printWarning(message: string): void {
  const c = getColors();
  console.log(`${c.yellow}${message}${c.reset}`);
}

/**
 * Print an info message in cyan.
 */
export function printInfo(message: string): void {
  const c = getColors();
  console.log(`${c.cyan}${message}${c.reset}`);
}

/**
 * Print a dimmed message.
 */
export function printDim(message: string): void {
  const c = getColors();
  console.log(`${c.dim}${message}${c.reset}`);
}

// ============================================================================
// Connection String Parsing
// ============================================================================

/**
 * Parsed MongoDB connection target information
 */
export interface ParsedTarget {
  /** Target hostname */
  host: string;
  /** Target port number */
  port: number;
  /** Database name (if specified) */
  database?: string;
  /** Auth source database */
  authSource?: string;
  /** Username for authentication */
  username?: string;
  /** Password for authentication */
  password?: string;
}

/**
 * Parse a MongoDB connection string or host:port format
 *
 * Supported formats:
 * - mongodb://host:port/database
 * - mongodb://user:pass@host:port/database?authSource=admin
 * - mongolake://host:port
 * - host:port
 * - host (defaults to port 27017)
 *
 * @param connectionString - Connection string to parse
 * @returns Parsed target information
 */
export function parseConnectionString(connectionString: string): ParsedTarget {
  // Check for mongodb:// or mongolake:// prefix
  if (connectionString.startsWith('mongodb://') || connectionString.startsWith('mongolake://')) {
    const url = new URL(connectionString);

    return {
      host: url.hostname || 'localhost',
      port: parseInt(url.port, 10) || 27017,
      database: url.pathname?.slice(1) || undefined,
      authSource: url.searchParams.get('authSource') || undefined,
      username: url.username || undefined,
      password: url.password || undefined,
    };
  }

  // Check for host:port format
  const hostPortMatch = connectionString.match(/^([^:]+):(\d+)$/);
  if (hostPortMatch) {
    return {
      host: hostPortMatch[1]!,
      port: parseInt(hostPortMatch[2]!, 10),
    };
  }

  // Just a hostname, use default MongoDB port
  return {
    host: connectionString,
    port: 27017,
  };
}

// ============================================================================
// Timestamp Utilities
// ============================================================================

/**
 * Get current ISO timestamp string
 */
export function getTimestamp(): string {
  return new Date().toISOString();
}
