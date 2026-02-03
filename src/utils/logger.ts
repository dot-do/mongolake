/**
 * Structured Logging Framework for MongoLake
 *
 * Provides a centralized logging facility with:
 * - Log levels: debug, info, warn, error
 * - Structured JSON output with timestamp, level, message, context
 * - Request ID tracking for distributed tracing
 * - Environment-aware configuration via ENVIRONMENT and LOG_LEVEL env vars
 *
 * @module utils/logger
 */

// ============================================================================
// Types
// ============================================================================

/**
 * Supported log levels in ascending order of severity.
 */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

/**
 * Numeric priority for log levels (lower = more verbose).
 */
const LOG_LEVEL_PRIORITY: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

/**
 * Context object for structured logging.
 * Can contain any additional metadata about the log entry.
 */
export interface LogContext {
  /** Request ID for distributed tracing */
  requestId?: string;
  /** Shard identifier */
  shardId?: string | number;
  /** Collection name */
  collection?: string;
  /** Database name */
  database?: string;
  /** Operation type */
  operation?: string;
  /** Duration in milliseconds */
  durationMs?: number;
  /** Error object or message */
  error?: unknown;
  /** Stack trace */
  stack?: string;
  /** Any additional fields */
  [key: string]: unknown;
}

/**
 * Structured log entry format.
 */
export interface LogEntry {
  /** ISO 8601 timestamp */
  timestamp: string;
  /** Log level */
  level: LogLevel;
  /** Log message */
  message: string;
  /** Additional context */
  context?: LogContext;
}

/**
 * Logger configuration options.
 */
export interface LoggerConfig {
  /** Minimum log level to output (default: based on ENVIRONMENT) */
  level?: LogLevel;
  /** Environment name (default: from ENVIRONMENT env var) */
  environment?: string;
  /** Whether to output as JSON (default: true in production, false in development) */
  jsonOutput?: boolean;
  /** Custom output function (default: console methods) */
  output?: LoggerOutput;
  /** Default context to include in all log entries */
  defaultContext?: LogContext;
}

/**
 * Custom output interface for logger.
 */
export interface LoggerOutput {
  debug(entry: LogEntry): void;
  info(entry: LogEntry): void;
  warn(entry: LogEntry): void;
  error(entry: LogEntry): void;
}

// ============================================================================
// Environment Detection
// ============================================================================

/**
 * Get the current environment from env vars.
 * Supports both Node.js and Cloudflare Workers environments.
 */
function getEnvironment(): string {
  // Try Node.js process.env first
  if (typeof process !== 'undefined' && process.env?.ENVIRONMENT) {
    return process.env.ENVIRONMENT;
  }
  // Default to production for safety
  return 'production';
}

/**
 * Get the configured log level from env vars.
 */
function getLogLevel(): LogLevel {
  // Try Node.js process.env first
  if (typeof process !== 'undefined' && process.env?.LOG_LEVEL) {
    const level = process.env.LOG_LEVEL.toLowerCase() as LogLevel;
    if (level in LOG_LEVEL_PRIORITY) {
      return level;
    }
  }
  // Default based on environment
  const env = getEnvironment();
  return env === 'development' || env === 'test' ? 'debug' : 'info';
}

/**
 * Check if JSON output should be used.
 */
function shouldUseJsonOutput(): boolean {
  const env = getEnvironment();
  return env === 'production' || env === 'staging';
}

// ============================================================================
// Request ID Tracking
// ============================================================================

/**
 * AsyncLocalStorage-like context for request ID tracking.
 * Falls back to a simple global for environments without AsyncLocalStorage.
 */
let currentRequestId: string | undefined;

/**
 * Set the current request ID for the execution context.
 */
export function setRequestId(requestId: string): void {
  currentRequestId = requestId;
}

/**
 * Get the current request ID from the execution context.
 */
export function getRequestId(): string | undefined {
  return currentRequestId;
}

/**
 * Clear the current request ID.
 */
export function clearRequestId(): void {
  currentRequestId = undefined;
}

/**
 * Generate a new request ID.
 */
export function generateRequestId(): string {
  const timestamp = Date.now().toString(36);
  const random = Math.random().toString(36).substring(2, 10);
  return `req-${timestamp}-${random}`;
}

/**
 * Execute a function with a request ID context.
 * Automatically sets and clears the request ID.
 */
export async function withRequestId<T>(
  requestId: string,
  fn: () => T | Promise<T>
): Promise<T> {
  const previousRequestId = currentRequestId;
  setRequestId(requestId);
  try {
    return await fn();
  } finally {
    if (previousRequestId !== undefined) {
      setRequestId(previousRequestId);
    } else {
      clearRequestId();
    }
  }
}

// ============================================================================
// Logger Class
// ============================================================================

/**
 * Structured logger with configurable output and context.
 */
export class Logger {
  private readonly level: LogLevel;
  private readonly levelPriority: number;
  private readonly environment: string;
  private readonly jsonOutput: boolean;
  private readonly output: LoggerOutput;
  private readonly defaultContext: LogContext;

  constructor(config: LoggerConfig = {}) {
    this.environment = config.environment ?? getEnvironment();
    this.level = config.level ?? getLogLevel();
    this.levelPriority = LOG_LEVEL_PRIORITY[this.level];
    this.jsonOutput = config.jsonOutput ?? shouldUseJsonOutput();
    this.defaultContext = config.defaultContext ?? {};
    this.output = config.output ?? this.createDefaultOutput();
  }

  /**
   * Create default output using console methods.
   */
  private createDefaultOutput(): LoggerOutput {
    const formatEntry = (entry: LogEntry): string => {
      if (this.jsonOutput) {
        return JSON.stringify(entry);
      }
      // Human-readable format for development
      const contextStr = entry.context
        ? ' ' + JSON.stringify(entry.context)
        : '';
      return `[${entry.timestamp}] ${entry.level.toUpperCase().padEnd(5)} ${entry.message}${contextStr}`;
    };

    return {
      debug: (entry) => console.debug(formatEntry(entry)),
      info: (entry) => console.info(formatEntry(entry)),
      warn: (entry) => console.warn(formatEntry(entry)),
      error: (entry) => console.error(formatEntry(entry)),
    };
  }

  /**
   * Check if a log level should be output.
   */
  private shouldLog(level: LogLevel): boolean {
    return LOG_LEVEL_PRIORITY[level] >= this.levelPriority;
  }

  /**
   * Build a log entry with timestamp and context.
   */
  private buildEntry(
    level: LogLevel,
    message: string,
    context?: LogContext
  ): LogEntry {
    const requestId = getRequestId();
    const mergedContext: LogContext = {
      ...this.defaultContext,
      ...context,
    };

    // Add request ID if available and not already present
    if (requestId && !mergedContext.requestId) {
      mergedContext.requestId = requestId;
    }

    // Add environment
    mergedContext.environment = this.environment;

    // Process error objects
    if (mergedContext.error instanceof Error) {
      mergedContext.error = mergedContext.error.message;
      if (!mergedContext.stack) {
        mergedContext.stack = (context?.error as Error).stack;
      }
    }

    const entry: LogEntry = {
      timestamp: new Date().toISOString(),
      level,
      message,
    };

    // Only include context if it has values
    if (Object.keys(mergedContext).length > 0) {
      entry.context = mergedContext;
    }

    return entry;
  }

  /**
   * Log a debug message.
   */
  debug(message: string, context?: LogContext): void {
    if (this.shouldLog('debug')) {
      this.output.debug(this.buildEntry('debug', message, context));
    }
  }

  /**
   * Log an info message.
   */
  info(message: string, context?: LogContext): void {
    if (this.shouldLog('info')) {
      this.output.info(this.buildEntry('info', message, context));
    }
  }

  /**
   * Log a warning message.
   */
  warn(message: string, context?: LogContext): void {
    if (this.shouldLog('warn')) {
      this.output.warn(this.buildEntry('warn', message, context));
    }
  }

  /**
   * Log an error message.
   */
  error(message: string, context?: LogContext): void {
    if (this.shouldLog('error')) {
      this.output.error(this.buildEntry('error', message, context));
    }
  }

  /**
   * Create a child logger with additional default context.
   */
  child(additionalContext: LogContext): Logger {
    return new Logger({
      level: this.level,
      environment: this.environment,
      jsonOutput: this.jsonOutput,
      output: this.output,
      defaultContext: {
        ...this.defaultContext,
        ...additionalContext,
      },
    });
  }

  /**
   * Get the current log level.
   */
  getLevel(): LogLevel {
    return this.level;
  }

  /**
   * Get the current environment.
   */
  getEnvironment(): string {
    return this.environment;
  }
}

// ============================================================================
// Default Logger Instance
// ============================================================================

/**
 * Default logger instance for use throughout the application.
 * Configured based on ENVIRONMENT and LOG_LEVEL env vars.
 */
export const logger = new Logger();

/**
 * Create a new logger with custom configuration.
 */
export function createLogger(config?: LoggerConfig): Logger {
  return new Logger(config);
}

// ============================================================================
// Convenience Exports
// ============================================================================

/**
 * Log a debug message using the default logger.
 */
export const debug = (message: string, context?: LogContext): void =>
  logger.debug(message, context);

/**
 * Log an info message using the default logger.
 */
export const info = (message: string, context?: LogContext): void =>
  logger.info(message, context);

/**
 * Log a warning message using the default logger.
 */
export const warn = (message: string, context?: LogContext): void =>
  logger.warn(message, context);

/**
 * Log an error message using the default logger.
 */
export const error = (message: string, context?: LogContext): void =>
  logger.error(message, context);
