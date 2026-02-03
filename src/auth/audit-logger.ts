/**
 * Audit Logger
 *
 * Provides audit logging implementations for authentication events.
 * Supports console logging, in-memory logging (for testing), and composite logging.
 */

import type { AuditLogger, AuditLogEntry, AuthEventType } from './types.js';

// ============================================================================
// Console Audit Logger
// ============================================================================

/**
 * Console-based audit logger for development/testing
 */
export class ConsoleAuditLogger implements AuditLogger {
  private prefix: string;

  constructor(prefix: string = '[MongoLake Auth]') {
    this.prefix = prefix;
  }

  log(entry: AuditLogEntry): void {
    const timestamp = entry.timestamp.toISOString();
    const details = [
      entry.userId ? `user=${entry.userId}` : null,
      entry.ipAddress ? `ip=${entry.ipAddress}` : null,
      entry.requestPath ? `path=${entry.requestPath}` : null,
      entry.errorCode ? `error=${entry.errorCode}` : null,
    ].filter(Boolean).join(' ');

    console.log(`${this.prefix} ${timestamp} ${entry.eventType} ${details}`);
  }
}

// ============================================================================
// In-Memory Audit Logger
// ============================================================================

/**
 * In-memory audit logger for testing
 */
export class InMemoryAuditLogger implements AuditLogger {
  private entries: AuditLogEntry[] = [];
  private maxEntries: number;

  constructor(maxEntries: number = 1000) {
    this.maxEntries = maxEntries;
  }

  log(entry: AuditLogEntry): void {
    this.entries.push(entry);
    // Trim old entries if over limit
    if (this.entries.length > this.maxEntries) {
      this.entries.shift();
    }
  }

  getEntries(): AuditLogEntry[] {
    return [...this.entries];
  }

  getEntriesByType(eventType: AuthEventType): AuditLogEntry[] {
    return this.entries.filter((e) => e.eventType === eventType);
  }

  getEntriesByUser(userId: string): AuditLogEntry[] {
    return this.entries.filter((e) => e.userId === userId);
  }

  clear(): void {
    this.entries = [];
  }
}

// ============================================================================
// Composite Audit Logger
// ============================================================================

/**
 * Composite audit logger that logs to multiple destinations
 */
export class CompositeAuditLogger implements AuditLogger {
  private loggers: AuditLogger[];

  constructor(loggers: AuditLogger[]) {
    this.loggers = loggers;
  }

  async log(entry: AuditLogEntry): Promise<void> {
    await Promise.all(this.loggers.map((logger) => logger.log(entry)));
  }
}
