/**
 * Shell Connection Management
 *
 * Handles MongoLake client connection with:
 * - Retry logic for transient errors
 * - Graceful error handling
 * - Connection status display
 *
 * @module cli/shell/connection
 */

import type { MongoLake } from '../../client/index.js';
import { getExtendedColors } from '../utils.js';

// ============================================================================
// Constants
// ============================================================================

/** Error codes that indicate transient network issues */
const TRANSIENT_ERRORS = ['ETIMEDOUT', 'ECONNRESET', 'ENOTFOUND'];

/** Maximum connection retry attempts */
const MAX_RETRIES = 3;

/** Delay between retries in production (ms) */
const RETRY_DELAY_PRODUCTION = 1000;

/** Delay between retries in test environment (ms) */
const RETRY_DELAY_TEST = 10;

// ============================================================================
// Types
// ============================================================================

/**
 * Connection options
 */
export interface ConnectionOptions {
  url?: string;
  timeout?: number;
}

/**
 * Extended MongoLake interface for shell operations.
 * The shell may call optional methods that aren't part of the core API.
 */
interface MongoLakeShellExtensions {
  connect?: (opts?: ConnectionOptions) => Promise<void>;
  serverInfo?: () => Promise<{ version: string }>;
}

// ============================================================================
// Connection with Retry
// ============================================================================

/**
 * Get retry delay based on environment.
 */
function getRetryDelay(): number {
  return process.env.NODE_ENV === 'test' || process.env.VITEST
    ? RETRY_DELAY_TEST
    : RETRY_DELAY_PRODUCTION;
}

/**
 * Check if an error is transient (worth retrying).
 */
function isTransientError(error: Error): boolean {
  return TRANSIENT_ERRORS.some(code => error.message.includes(code));
}

/**
 * Connect to MongoLake with retry logic for transient errors.
 *
 * @param lake - MongoLake client instance
 * @param options - Connection options
 * @param verbose - Whether to log retry attempts
 * @throws Error if connection fails after all retries
 */
export async function connectWithRetry(
  lake: MongoLake,
  options: ConnectionOptions,
  verbose: boolean
): Promise<void> {
  let lastError: Error | null = null;
  const retryDelay = getRetryDelay();

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      // Access optional connect method for shell compatibility
      const shellLake = lake as MongoLake & MongoLakeShellExtensions;
      if (shellLake.connect) {
        await shellLake.connect(options);
      }
      return;
    } catch (error) {
      lastError = error as Error;

      if (verbose) {
        console.error(`Connection attempt ${attempt} failed:`, lastError.message);
      }

      // Only retry on transient errors
      if (isTransientError(lastError) && attempt < MAX_RETRIES) {
        await sleep(retryDelay);
        continue;
      }

      break;
    }
  }

  throw lastError;
}

/**
 * Handle connection error and display appropriate message.
 *
 * @param error - Connection error
 * @throws Re-throws the error after displaying message
 */
export function handleConnectionError(error: Error): never {
  const colors = getExtendedColors();
  const errorMessage = error.message;

  if (errorMessage.includes('ECONNREFUSED')) {
    console.error(`${colors.red}Could not connect to MongoLake server${colors.reset}`);
    console.log(`${colors.dim}Make sure the mongolake dev server is running${colors.reset}`);
  } else if (errorMessage.includes('Authentication failed')) {
    console.error(`${colors.red}Authentication failed${colors.reset}`);
  } else {
    console.error(`${colors.red}Could not connect: ${errorMessage}${colors.reset}`);
  }

  throw error;
}

// ============================================================================
// Utilities
// ============================================================================

/**
 * Sleep for a specified duration.
 */
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
