/**
 * Corruption Audit Trail Tests (Storage Layer)
 *
 * Tests for the persistent corruption audit trail feature.
 * When skipCorruptedFiles is enabled, all skipped files should be logged
 * to an audit trail that can be retrieved and analyzed.
 *
 * This tests the CorruptionAudit class which provides:
 * - Real-time corruption event tracking
 * - Persistent audit log across multiple queries
 * - Query-specific corruption summaries
 * - Integration with external monitoring systems
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  CorruptionAudit,
  createCorruptionAudit,
  type CorruptionAuditEntry,
  type CorruptionSummary,
} from '../../../src/client/corruption-audit.js';
import type { CorruptionReport } from '../../../src/types.js';

// ============================================================================
// Test Helpers
// ============================================================================

function createSampleAuditEntry(
  overrides?: Partial<Omit<CorruptionAuditEntry, 'timestamp'>>
): Omit<CorruptionAuditEntry, 'timestamp'> {
  return {
    filePath: 'testdb/users/data_001.parquet',
    error: 'Invalid Parquet magic bytes',
    query: {
      collection: 'users',
      database: 'testdb',
      filter: { status: 'active' },
      operation: 'find',
    },
    ...overrides,
  };
}

function createSampleCorruptionReport(
  overrides?: Partial<CorruptionReport>
): CorruptionReport {
  return {
    filename: 'testdb/users/data_001.parquet',
    error: 'Invalid Parquet magic bytes',
    timestamp: new Date(),
    collection: 'users',
    database: 'testdb',
    ...overrides,
  };
}

// ============================================================================
// Audit Entry Creation Tests - skipCorruptedFiles creates audit entries
// ============================================================================

describe('Corruption Audit Trail - Entry Creation', () => {
  let audit: CorruptionAudit;

  beforeEach(() => {
    audit = new CorruptionAudit();
  });

  it('should create an audit entry with file path when corruption is detected', () => {
    const entry = createSampleAuditEntry({ filePath: 'mydb/orders/data_123.parquet' });

    audit.log(entry);

    const entries = audit.getEntries();
    expect(entries).toHaveLength(1);
    expect(entries[0].filePath).toBe('mydb/orders/data_123.parquet');
  });

  it('should include error details in the audit entry', () => {
    const entry = createSampleAuditEntry({
      error: 'Footer checksum mismatch: expected 0xABCD1234, got 0xDEADBEEF',
    });

    audit.log(entry);

    const entries = audit.getEntries();
    expect(entries[0].error).toBe('Footer checksum mismatch: expected 0xABCD1234, got 0xDEADBEEF');
    expect(entries[0].error).toContain('checksum');
  });

  it('should automatically add timestamp to audit entry', () => {
    const beforeLog = new Date();

    audit.log(createSampleAuditEntry());

    const afterLog = new Date();
    const entries = audit.getEntries();

    expect(entries[0].timestamp).toBeInstanceOf(Date);
    expect(entries[0].timestamp.getTime()).toBeGreaterThanOrEqual(beforeLog.getTime());
    expect(entries[0].timestamp.getTime()).toBeLessThanOrEqual(afterLog.getTime());
  });

  it('should include query context with collection name', () => {
    const entry = createSampleAuditEntry({
      query: {
        collection: 'products',
        operation: 'find',
      },
    });

    audit.log(entry);

    const entries = audit.getEntries();
    expect(entries[0].query.collection).toBe('products');
  });

  it('should include query context with filter when provided', () => {
    const entry = createSampleAuditEntry({
      query: {
        collection: 'users',
        filter: { age: { $gte: 18 }, status: 'active' },
        operation: 'find',
      },
    });

    audit.log(entry);

    const entries = audit.getEntries();
    expect(entries[0].query.filter).toEqual({ age: { $gte: 18 }, status: 'active' });
  });

  it('should include operation type in query context', () => {
    audit.log(createSampleAuditEntry({
      query: { collection: 'users', operation: 'find' },
    }));
    audit.log(createSampleAuditEntry({
      query: { collection: 'users', operation: 'findOne' },
    }));
    audit.log(createSampleAuditEntry({
      query: { collection: 'users', operation: 'aggregate' },
    }));

    const entries = audit.getEntries();
    expect(entries).toHaveLength(3);
    expect(entries[0].query.operation).toBe('find');
    expect(entries[1].query.operation).toBe('findOne');
    expect(entries[2].query.operation).toBe('aggregate');
  });

  it('should include database name in query context', () => {
    const entry = createSampleAuditEntry({
      query: {
        collection: 'users',
        database: 'production_db',
        operation: 'find',
      },
    });

    audit.log(entry);

    const entries = audit.getEntries();
    expect(entries[0].query.database).toBe('production_db');
  });
});

// ============================================================================
// Audit Retrieval Tests - getCorruptionAudit() method
// ============================================================================

describe('Corruption Audit Trail - Audit Retrieval via getEntries()', () => {
  let audit: CorruptionAudit;

  beforeEach(() => {
    audit = new CorruptionAudit();
  });

  it('should return empty array when no corruption has occurred', () => {
    const entries = audit.getEntries();

    expect(entries).toEqual([]);
    expect(entries).toHaveLength(0);
  });

  it('should retrieve all audit entries in chronological order', async () => {
    audit.log(createSampleAuditEntry({ filePath: 'first.parquet' }));
    await new Promise((r) => setTimeout(r, 10)); // Small delay
    audit.log(createSampleAuditEntry({ filePath: 'second.parquet' }));
    await new Promise((r) => setTimeout(r, 10));
    audit.log(createSampleAuditEntry({ filePath: 'third.parquet' }));

    const entries = audit.getEntries();

    expect(entries).toHaveLength(3);
    expect(entries[0].filePath).toBe('first.parquet');
    expect(entries[1].filePath).toBe('second.parquet');
    expect(entries[2].filePath).toBe('third.parquet');

    // Verify chronological order
    expect(entries[0].timestamp.getTime()).toBeLessThanOrEqual(entries[1].timestamp.getTime());
    expect(entries[1].timestamp.getTime()).toBeLessThanOrEqual(entries[2].timestamp.getTime());
  });

  it('should filter audit entries by collection', () => {
    audit.log(createSampleAuditEntry({
      filePath: 'users_1.parquet',
      query: { collection: 'users', operation: 'find' },
    }));
    audit.log(createSampleAuditEntry({
      filePath: 'orders_1.parquet',
      query: { collection: 'orders', operation: 'find' },
    }));
    audit.log(createSampleAuditEntry({
      filePath: 'users_2.parquet',
      query: { collection: 'users', operation: 'findOne' },
    }));

    const usersEntries = audit.getEntriesByCollection('users');

    expect(usersEntries).toHaveLength(2);
    expect(usersEntries.every((e) => e.query.collection === 'users')).toBe(true);
  });

  it('should filter audit entries by database', () => {
    audit.log(createSampleAuditEntry({
      query: { collection: 'users', database: 'prod', operation: 'find' },
    }));
    audit.log(createSampleAuditEntry({
      query: { collection: 'orders', database: 'staging', operation: 'find' },
    }));
    audit.log(createSampleAuditEntry({
      query: { collection: 'products', database: 'prod', operation: 'find' },
    }));

    const prodEntries = audit.getEntriesByDatabase('prod');

    expect(prodEntries).toHaveLength(2);
    expect(prodEntries.every((e) => e.query.database === 'prod')).toBe(true);
  });

  it('should filter audit entries by time range', async () => {
    const start = new Date();
    await new Promise((r) => setTimeout(r, 10));

    audit.log(createSampleAuditEntry({ filePath: 'in_range.parquet' }));

    await new Promise((r) => setTimeout(r, 10));
    const end = new Date();

    await new Promise((r) => setTimeout(r, 10));
    audit.log(createSampleAuditEntry({ filePath: 'out_of_range.parquet' }));

    const rangeEntries = audit.getEntriesByTimeRange(start, end);

    expect(rangeEntries).toHaveLength(1);
    expect(rangeEntries[0].filePath).toBe('in_range.parquet');
  });

  it('should filter audit entries by file path', () => {
    const sharedPath = 'db/users/data.parquet';
    audit.log(createSampleAuditEntry({ filePath: sharedPath }));
    audit.log(createSampleAuditEntry({ filePath: 'other/file.parquet' }));
    audit.log(createSampleAuditEntry({ filePath: sharedPath }));

    const fileEntries = audit.getEntriesByFile(sharedPath);

    expect(fileEntries).toHaveLength(2);
    expect(fileEntries.every((e) => e.filePath === sharedPath)).toBe(true);
  });

  it('should return empty array when no entries match filter', () => {
    audit.log(createSampleAuditEntry({
      query: { collection: 'users', operation: 'find' },
    }));

    const ordersEntries = audit.getEntriesByCollection('orders');

    expect(ordersEntries).toEqual([]);
  });
});

// ============================================================================
// Multiple Corrupted Files Tests - all logged
// ============================================================================

describe('Corruption Audit Trail - Multiple Corrupted Files', () => {
  let audit: CorruptionAudit;

  beforeEach(() => {
    audit = new CorruptionAudit();
  });

  it('should log all corrupted files from a single query', () => {
    // Simulate finding multiple corrupted files in one query
    const corruptedFiles = [
      { filePath: 'db/users/part_001.parquet', error: 'Invalid magic bytes' },
      { filePath: 'db/users/part_002.parquet', error: 'Truncated file' },
      { filePath: 'db/users/part_003.parquet', error: 'Checksum mismatch' },
    ];

    for (const { filePath, error } of corruptedFiles) {
      audit.log({
        filePath,
        error,
        query: { collection: 'users', filter: {}, operation: 'find' },
      });
    }

    const entries = audit.getEntries();
    expect(entries).toHaveLength(3);

    // Verify each file is logged with its specific error
    expect(entries.find((e) => e.filePath.includes('part_001'))?.error).toBe('Invalid magic bytes');
    expect(entries.find((e) => e.filePath.includes('part_002'))?.error).toBe('Truncated file');
    expect(entries.find((e) => e.filePath.includes('part_003'))?.error).toBe('Checksum mismatch');
  });

  it('should preserve audit entries across multiple queries', () => {
    // First query
    audit.log(createSampleAuditEntry({
      filePath: 'db/orders/q1_file.parquet',
      query: { collection: 'orders', operation: 'find' },
    }));

    // Second query
    audit.log(createSampleAuditEntry({
      filePath: 'db/products/q2_file.parquet',
      query: { collection: 'products', operation: 'findOne' },
    }));

    const entries = audit.getEntries();
    expect(entries).toHaveLength(2);
    expect(entries.find((e) => e.query.collection === 'orders')).toBeDefined();
    expect(entries.find((e) => e.query.collection === 'products')).toBeDefined();
  });

  it('should track same file corrupted in different queries', () => {
    const sharedFilePath = 'db/users/data.parquet';

    // Log corruption from first query
    audit.log({
      filePath: sharedFilePath,
      error: 'Corruption detected',
      query: { collection: 'users', filter: { status: 'active' }, operation: 'find' },
    });

    // Log corruption from second query (same file, different filter)
    audit.log({
      filePath: sharedFilePath,
      error: 'Corruption detected',
      query: { collection: 'users', filter: { status: 'inactive' }, operation: 'find' },
    });

    const entries = audit.getEntries();
    expect(entries).toHaveLength(2);

    // Both entries should reference the same file
    const samePaths = entries.filter((e) => e.filePath === sharedFilePath);
    expect(samePaths).toHaveLength(2);

    // But with different query contexts
    const filters = samePaths.map((e) => e.query.filter);
    expect(filters).toContainEqual({ status: 'active' });
    expect(filters).toContainEqual({ status: 'inactive' });
  });

  it('should handle high volume of corrupted files', () => {
    // Log 100 corrupted files
    for (let i = 0; i < 100; i++) {
      audit.log(createSampleAuditEntry({
        filePath: `db/collection/file_${i.toString().padStart(3, '0')}.parquet`,
        error: `Error ${i}`,
      }));
    }

    expect(audit.size).toBe(100);
    expect(audit.getEntries()).toHaveLength(100);
  });
});

// ============================================================================
// Summary Tests - getSummary() method
// ============================================================================

describe('Corruption Audit Trail - Summary', () => {
  let audit: CorruptionAudit;

  beforeEach(() => {
    audit = new CorruptionAudit();
  });

  it('should return summary with zero counts when no corruption', () => {
    const summary = audit.getSummary();

    expect(summary.totalCorrupted).toBe(0);
    expect(summary.uniqueFiles).toBe(0);
    expect(summary.affectedCollections).toEqual([]);
    expect(summary.timeRange).toBeNull();
    expect(summary.hasDataLoss).toBe(false);
  });

  it('should return accurate corruption count', () => {
    audit.log(createSampleAuditEntry({ filePath: 'file1.parquet' }));
    audit.log(createSampleAuditEntry({ filePath: 'file2.parquet' }));
    audit.log(createSampleAuditEntry({ filePath: 'file3.parquet' }));

    const summary = audit.getSummary();
    expect(summary.totalCorrupted).toBe(3);
  });

  it('should track unique files correctly', () => {
    // Same file corrupted twice
    audit.log(createSampleAuditEntry({ filePath: 'file1.parquet' }));
    audit.log(createSampleAuditEntry({ filePath: 'file1.parquet' }));
    audit.log(createSampleAuditEntry({ filePath: 'file2.parquet' }));

    const summary = audit.getSummary();
    expect(summary.totalCorrupted).toBe(3);
    expect(summary.uniqueFiles).toBe(2);
  });

  it('should list all affected collections', () => {
    audit.log(createSampleAuditEntry({
      query: { collection: 'users', operation: 'find' },
    }));
    audit.log(createSampleAuditEntry({
      query: { collection: 'orders', operation: 'find' },
    }));
    audit.log(createSampleAuditEntry({
      query: { collection: 'users', operation: 'findOne' },
    }));

    const summary = audit.getSummary();
    expect(summary.affectedCollections).toContain('users');
    expect(summary.affectedCollections).toContain('orders');
    expect(summary.affectedCollections).toHaveLength(2);
  });

  it('should calculate correct time range', async () => {
    const beforeFirst = new Date();
    await new Promise((r) => setTimeout(r, 5));

    audit.log(createSampleAuditEntry({ filePath: 'first.parquet' }));
    await new Promise((r) => setTimeout(r, 50));
    audit.log(createSampleAuditEntry({ filePath: 'last.parquet' }));

    await new Promise((r) => setTimeout(r, 5));
    const afterLast = new Date();

    const summary = audit.getSummary();
    expect(summary.timeRange).not.toBeNull();
    expect(summary.timeRange!.start.getTime()).toBeGreaterThan(beforeFirst.getTime());
    expect(summary.timeRange!.end.getTime()).toBeLessThan(afterLast.getTime());
    expect(summary.timeRange!.start.getTime()).toBeLessThan(summary.timeRange!.end.getTime());
  });

  it('should indicate data loss when files are corrupted', () => {
    audit.log(createSampleAuditEntry());

    const summary = audit.getSummary();
    expect(summary.hasDataLoss).toBe(true);
  });

  it('should get summary for specific collection', () => {
    audit.log(createSampleAuditEntry({
      filePath: 'users_1.parquet',
      query: { collection: 'users', operation: 'find' },
    }));
    audit.log(createSampleAuditEntry({
      filePath: 'orders_1.parquet',
      query: { collection: 'orders', operation: 'find' },
    }));
    audit.log(createSampleAuditEntry({
      filePath: 'users_2.parquet',
      query: { collection: 'users', operation: 'find' },
    }));

    const usersSummary = audit.getSummaryByCollection('users');
    expect(usersSummary.totalCorrupted).toBe(2);
    expect(usersSummary.affectedCollections).toEqual(['users']);
  });
});

// ============================================================================
// Management Tests - clear, export, import
// ============================================================================

describe('Corruption Audit Trail - Management', () => {
  let audit: CorruptionAudit;

  beforeEach(() => {
    audit = new CorruptionAudit();
  });

  it('should clear all audit entries', () => {
    audit.log(createSampleAuditEntry({ filePath: 'file1.parquet' }));
    audit.log(createSampleAuditEntry({ filePath: 'file2.parquet' }));

    expect(audit.size).toBe(2);

    audit.clear();

    expect(audit.size).toBe(0);
    expect(audit.getEntries()).toEqual([]);
    expect(audit.isEmpty).toBe(true);
  });

  it('should clear entries older than specified date', async () => {
    audit.log(createSampleAuditEntry({ filePath: 'old.parquet' }));

    await new Promise((r) => setTimeout(r, 50));
    const cutoff = new Date();
    await new Promise((r) => setTimeout(r, 10));

    audit.log(createSampleAuditEntry({ filePath: 'new.parquet' }));

    const removed = audit.clearOlderThan(cutoff);

    expect(removed).toBe(1);
    expect(audit.size).toBe(1);
    expect(audit.getEntries()[0].filePath).toBe('new.parquet');
  });

  it('should export audit to valid JSON', () => {
    audit.log(createSampleAuditEntry({
      filePath: 'test.parquet',
      error: 'Test error with "quotes" and special chars: \n\t',
      query: {
        collection: 'users',
        filter: { name: "O'Brien" },
        operation: 'find',
      },
    }));

    const exported = audit.toJSON();

    // Should be valid JSON
    expect(() => JSON.parse(exported)).not.toThrow();

    const parsed = JSON.parse(exported);
    expect(parsed).toHaveLength(1);
    expect(parsed[0].query.filter.name).toBe("O'Brien");
  });

  it('should export audit with ISO 8601 timestamps', () => {
    audit.log(createSampleAuditEntry());

    const exported = audit.toJSON();
    const parsed = JSON.parse(exported);

    // ISO 8601 format: YYYY-MM-DDTHH:mm:ss.sssZ
    const isoRegex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z$/;
    expect(parsed[0].timestamp).toMatch(isoRegex);
  });

  it('should import audit entries from JSON', () => {
    const exportedData = JSON.stringify([{
      filePath: 'imported.parquet',
      error: 'Imported error',
      timestamp: new Date().toISOString(),
      query: { collection: 'imported', operation: 'find' },
    }]);

    const count = audit.fromJSON(exportedData);

    expect(count).toBe(1);
    expect(audit.size).toBe(1);
    expect(audit.getEntries()[0].filePath).toBe('imported.parquet');
    expect(audit.getEntries()[0].timestamp).toBeInstanceOf(Date);
  });

  it('should merge imported entries with existing ones', () => {
    audit.log(createSampleAuditEntry({ filePath: 'existing.parquet' }));

    const exportedData = JSON.stringify([{
      filePath: 'imported.parquet',
      error: 'Imported error',
      timestamp: new Date().toISOString(),
      query: { collection: 'imported', operation: 'find' },
    }]);

    audit.fromJSON(exportedData);

    expect(audit.size).toBe(2);
  });
});

// ============================================================================
// logFromReport Tests - Integration with CorruptionReport
// ============================================================================

describe('Corruption Audit Trail - logFromReport Integration', () => {
  let audit: CorruptionAudit;

  beforeEach(() => {
    audit = new CorruptionAudit();
  });

  it('should log entry from CorruptionReport', () => {
    const report = createSampleCorruptionReport({
      filename: 'testdb/users/corrupted.parquet',
      error: 'Magic bytes mismatch',
      collection: 'users',
      database: 'testdb',
    });

    audit.logFromReport(report, {
      filter: { status: 'active' },
      operation: 'find',
    });

    const entries = audit.getEntries();
    expect(entries).toHaveLength(1);
    expect(entries[0].filePath).toBe('testdb/users/corrupted.parquet');
    expect(entries[0].error).toBe('Magic bytes mismatch');
    expect(entries[0].query.collection).toBe('users');
    expect(entries[0].query.database).toBe('testdb');
    expect(entries[0].query.filter).toEqual({ status: 'active' });
    expect(entries[0].query.operation).toBe('find');
  });

  it('should handle report without database', () => {
    const report = createSampleCorruptionReport({
      database: undefined,
    });

    audit.logFromReport(report, { operation: 'find' });

    const entries = audit.getEntries();
    expect(entries[0].query.database).toBeUndefined();
  });
});

// ============================================================================
// createCallback Tests - Callback factory
// ============================================================================

describe('Corruption Audit Trail - createCallback', () => {
  let audit: CorruptionAudit;

  beforeEach(() => {
    audit = new CorruptionAudit();
  });

  it('should create callback that logs corruption reports', () => {
    const callback = audit.createCallback({
      filter: { _id: 'test123' },
      operation: 'findOne',
    });

    const report = createSampleCorruptionReport();
    callback(report);

    const entries = audit.getEntries();
    expect(entries).toHaveLength(1);
    expect(entries[0].query.operation).toBe('findOne');
    expect(entries[0].query.filter).toEqual({ _id: 'test123' });
  });

  it('should handle multiple callbacks independently', () => {
    const callback1 = audit.createCallback({ operation: 'find' });
    const callback2 = audit.createCallback({ operation: 'aggregate' });

    callback1(createSampleCorruptionReport({ filename: 'file1.parquet' }));
    callback2(createSampleCorruptionReport({ filename: 'file2.parquet' }));

    const entries = audit.getEntries();
    expect(entries).toHaveLength(2);
    expect(entries[0].query.operation).toBe('find');
    expect(entries[1].query.operation).toBe('aggregate');
  });
});

// ============================================================================
// Options Tests - Configuration options
// ============================================================================

describe('Corruption Audit Trail - Options', () => {
  it('should respect maxEntries option', () => {
    const audit = new CorruptionAudit({ maxEntries: 5 });

    for (let i = 0; i < 10; i++) {
      audit.log(createSampleAuditEntry({ filePath: `file_${i}.parquet` }));
    }

    expect(audit.size).toBe(5);
    // Should keep the most recent entries
    const entries = audit.getEntries();
    expect(entries[0].filePath).toBe('file_5.parquet');
    expect(entries[4].filePath).toBe('file_9.parquet');
  });

  it('should include stack traces when configured', () => {
    const audit = new CorruptionAudit({ includeStackTraces: true });

    audit.log({
      ...createSampleAuditEntry(),
      errorStack: 'Error: Test\n    at test.ts:1:1',
    });

    const entries = audit.getEntries();
    expect(entries[0].errorStack).toBe('Error: Test\n    at test.ts:1:1');
  });

  it('should strip stack traces when not configured', () => {
    const audit = new CorruptionAudit({ includeStackTraces: false });

    audit.log({
      ...createSampleAuditEntry(),
      errorStack: 'Error: Test\n    at test.ts:1:1',
    });

    const entries = audit.getEntries();
    expect(entries[0].errorStack).toBeUndefined();
  });

  it('should use default maxEntries of 10000', () => {
    const audit = new CorruptionAudit();

    // Log entries up to the limit
    for (let i = 0; i < 10001; i++) {
      audit.log(createSampleAuditEntry({ filePath: `file_${i}.parquet` }));
    }

    expect(audit.size).toBe(10000);
  });
});

// ============================================================================
// Edge Cases Tests
// ============================================================================

describe('Corruption Audit Trail - Edge Cases', () => {
  let audit: CorruptionAudit;

  beforeEach(() => {
    audit = new CorruptionAudit();
  });

  it('should handle very long file paths', () => {
    const longPath = 'a'.repeat(1000) + '/file.parquet';

    audit.log(createSampleAuditEntry({ filePath: longPath }));

    const entries = audit.getEntries();
    expect(entries[0].filePath).toBe(longPath);
  });

  it('should handle very long error messages', () => {
    const longError = 'Error: ' + 'x'.repeat(10000);

    audit.log(createSampleAuditEntry({ error: longError }));

    const entries = audit.getEntries();
    expect(entries[0].error).toBe(longError);
  });

  it('should handle special characters in file paths', () => {
    const specialPaths = [
      'db/col/file with spaces.parquet',
      'db/col/file-with-dashes.parquet',
      'db/col/file_with_underscores.parquet',
      'db/col/file.multiple.dots.parquet',
      'db/col/unicode-\u4e2d\u6587.parquet',
    ];

    for (const filePath of specialPaths) {
      audit.log(createSampleAuditEntry({ filePath }));
    }

    const entries = audit.getEntries();
    expect(entries).toHaveLength(specialPaths.length);

    for (const filePath of specialPaths) {
      expect(entries.some((e) => e.filePath === filePath)).toBe(true);
    }
  });

  it('should handle empty collection name', () => {
    audit.log({
      filePath: 'file.parquet',
      error: 'Error',
      query: {
        collection: '',
        operation: 'find',
      },
    });

    const entries = audit.getEntries();
    expect(entries[0].query.collection).toBe('');
  });

  it('should handle concurrent audit logging', async () => {
    const promises = [];
    for (let i = 0; i < 100; i++) {
      promises.push(
        Promise.resolve().then(() => {
          audit.log(createSampleAuditEntry({
            filePath: `file_${i}.parquet`,
          }));
        })
      );
    }

    await Promise.all(promises);

    expect(audit.size).toBe(100);
  });

  it('should return copy of entries to prevent mutation', () => {
    audit.log(createSampleAuditEntry({ filePath: 'original.parquet' }));

    const entries = audit.getEntries();
    entries.push({
      filePath: 'mutated.parquet',
      error: 'Mutation attempt',
      timestamp: new Date(),
      query: { collection: 'test', operation: 'find' },
    });

    // Original audit should not be affected
    expect(audit.size).toBe(1);
    expect(audit.getEntries()).toHaveLength(1);
  });

  it('should track isEmpty correctly', () => {
    expect(audit.isEmpty).toBe(true);

    audit.log(createSampleAuditEntry());
    expect(audit.isEmpty).toBe(false);

    audit.clear();
    expect(audit.isEmpty).toBe(true);
  });
});

// ============================================================================
// Factory Function Tests
// ============================================================================

describe('Corruption Audit Trail - Factory Function', () => {
  it('should create audit instance with createCorruptionAudit()', () => {
    const audit = createCorruptionAudit();

    expect(audit).toBeInstanceOf(CorruptionAudit);
  });

  it('should pass options to createCorruptionAudit()', () => {
    const audit = createCorruptionAudit({ maxEntries: 10 });

    for (let i = 0; i < 20; i++) {
      audit.log(createSampleAuditEntry({ filePath: `file_${i}.parquet` }));
    }

    expect(audit.size).toBe(10);
  });
});

// ============================================================================
// skipCorruptedFiles Integration Tests
// ============================================================================

describe('Corruption Audit Trail - skipCorruptedFiles Integration', () => {
  let audit: CorruptionAudit;

  beforeEach(() => {
    audit = new CorruptionAudit();
  });

  it('should log audit entry when skipCorruptedFiles encounters corruption', () => {
    // Simulate what happens when a corrupted file is skipped during find()
    audit.log({
      filePath: 'testdb/users/data_corrupted.parquet',
      error: 'Invalid Parquet footer: magic bytes not found',
      query: {
        collection: 'users',
        filter: { status: 'active' },
        operation: 'find',
      },
    });

    const entries = audit.getEntries();
    expect(entries).toHaveLength(1);
    expect(entries[0].query.operation).toBe('find');
    expect(entries[0].error).toContain('magic bytes');
  });

  it('should log audit for findOne operation', () => {
    audit.log({
      filePath: 'testdb/users/shard_001.parquet',
      error: 'Checksum mismatch',
      query: {
        collection: 'users',
        filter: { _id: 'user123' },
        operation: 'findOne',
      },
    });

    const entries = audit.getEntries();
    expect(entries[0].query.operation).toBe('findOne');
    expect(entries[0].query.filter).toEqual({ _id: 'user123' });
  });

  it('should log audit for aggregate operation', () => {
    audit.log({
      filePath: 'testdb/orders/data.parquet',
      error: 'Column metadata corrupted',
      query: {
        collection: 'orders',
        operation: 'aggregate',
      },
    });

    const entries = audit.getEntries();
    expect(entries[0].query.operation).toBe('aggregate');
    expect(entries[0].query.filter).toBeUndefined();
  });

  it('should preserve complex filter structures in audit', () => {
    const complexFilter = {
      $and: [
        { status: { $in: ['active', 'pending'] } },
        { createdAt: { $gte: new Date('2024-01-01').toISOString() } },
        {
          $or: [
            { role: 'admin' },
            { permissions: { $elemMatch: { level: { $gt: 5 } } } },
          ],
        },
      ],
    };

    audit.log({
      filePath: 'file.parquet',
      error: 'Corruption detected',
      query: {
        collection: 'users',
        filter: complexFilter,
        operation: 'find',
      },
    });

    const entries = audit.getEntries();
    expect(entries[0].query.filter).toEqual(complexFilter);
  });
});
