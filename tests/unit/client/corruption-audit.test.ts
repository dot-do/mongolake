/**
 * Corruption Audit Trail Tests
 *
 * Tests for the skipCorruptedFiles option and corruption tracking:
 * - CorruptionReport interface
 * - onCorruptedFile callback
 * - QueryMetadata tracking
 * - getQueryMetadata() method
 */

import { describe, it, expect, vi } from 'vitest';
import { createTestCollection } from './test-helpers.js';
import type { CorruptionReport, QueryMetadata } from '../../../src/client/index.js';

describe('Corruption Audit Trail', () => {
  describe('getQueryMetadata()', () => {
    it('should return null before any query is executed', () => {
      const { collection } = createTestCollection();

      const metadata = collection.getQueryMetadata();

      expect(metadata).toBeNull();
    });

    it('should return metadata after a query is executed', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', name: 'Alice' });

      await collection.find({}).toArray();

      const metadata = collection.getQueryMetadata();
      expect(metadata).not.toBeNull();
      expect(metadata?.totalFilesProcessed).toBeGreaterThanOrEqual(0);
      expect(metadata?.corruptedFiles).toEqual([]);
      expect(metadata?.skippedCount).toBe(0);
      expect(metadata?.hasDataLoss).toBe(false);
    });

    it('should track corrupted files when skipCorruptedFiles is true', async () => {
      const { collection, storage } = createTestCollection();

      // Insert a document to create a valid parquet file
      await collection.insertOne({ _id: '1', name: 'Alice' });

      // Inject a corrupted file directly into storage
      const corruptedPath = 'testdb/users_corrupted_123.parquet';
      await storage.put(corruptedPath, new Uint8Array([0x00, 0x01, 0x02, 0x03])); // Invalid parquet data

      // Query with skipCorruptedFiles enabled
      const docs = await collection.find({}, { skipCorruptedFiles: true }).toArray();

      const metadata = collection.getQueryMetadata();
      expect(metadata).not.toBeNull();
      expect(metadata?.hasDataLoss).toBe(true);
      expect(metadata?.skippedCount).toBe(1);
      expect(metadata?.corruptedFiles).toHaveLength(1);

      const report = metadata?.corruptedFiles[0];
      expect(report?.filename).toBe(corruptedPath);
      expect(report?.collection).toBe('users');
      expect(report?.database).toBe('testdb');
      expect(report?.error).toBeDefined();
      expect(report?.timestamp).toBeInstanceOf(Date);

      // Should still return valid documents
      expect(docs).toHaveLength(1);
      expect(docs[0].name).toBe('Alice');
    });

    it('should throw by default when encountering corrupted files', async () => {
      const { collection, storage } = createTestCollection();

      // Inject a corrupted file
      const corruptedPath = 'testdb/users_corrupted_456.parquet';
      await storage.put(corruptedPath, new Uint8Array([0xFF, 0xFE, 0xFD]));

      // Should throw without skipCorruptedFiles
      await expect(collection.find({}).toArray()).rejects.toThrow(/Failed to read Parquet file/);
    });
  });

  describe('onCorruptedFile callback', () => {
    it('should call callback for each corrupted file', async () => {
      const { collection, storage } = createTestCollection();

      // Insert valid document
      await collection.insertOne({ _id: '1', name: 'Alice' });

      // Inject multiple corrupted files
      await storage.put('testdb/users_bad1.parquet', new Uint8Array([0x00]));
      await storage.put('testdb/users_bad2.parquet', new Uint8Array([0x01]));

      const reports: CorruptionReport[] = [];
      const callback = vi.fn((report: CorruptionReport) => {
        reports.push(report);
      });

      await collection.find({}, {
        skipCorruptedFiles: true,
        onCorruptedFile: callback,
      }).toArray();

      expect(callback).toHaveBeenCalledTimes(2);
      expect(reports).toHaveLength(2);

      // Verify callback received proper CorruptionReport objects
      for (const report of reports) {
        expect(report.filename).toBeDefined();
        expect(report.error).toBeDefined();
        expect(report.timestamp).toBeInstanceOf(Date);
        expect(report.collection).toBe('users');
        expect(report.database).toBe('testdb');
      }
    });

    it('should not call callback when no files are corrupted', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', name: 'Alice' });

      const callback = vi.fn();

      await collection.find({}, {
        skipCorruptedFiles: true,
        onCorruptedFile: callback,
      }).toArray();

      expect(callback).not.toHaveBeenCalled();
    });

    it('should call callback even if query is empty', async () => {
      const { collection, storage } = createTestCollection();

      // Only inject corrupted file, no valid data
      await storage.put('testdb/users_corrupt.parquet', new Uint8Array([0xAB]));

      const callback = vi.fn();

      const docs = await collection.find({}, {
        skipCorruptedFiles: true,
        onCorruptedFile: callback,
      }).toArray();

      expect(callback).toHaveBeenCalledTimes(1);
      expect(docs).toHaveLength(0);
    });
  });

  describe('QueryMetadata', () => {
    it('should track totalFilesProcessed correctly', async () => {
      const { collection } = createTestCollection();

      // Insert multiple documents to potentially create multiple files
      await collection.insertOne({ _id: '1', name: 'Alice' });
      await collection.insertOne({ _id: '2', name: 'Bob' });

      await collection.find({}).toArray();

      const metadata = collection.getQueryMetadata();
      expect(metadata?.totalFilesProcessed).toBeGreaterThanOrEqual(1);
    });

    it('should update metadata on each query', async () => {
      const { collection, storage } = createTestCollection();

      // First query - clean
      await collection.insertOne({ _id: '1', name: 'Alice' });
      await collection.find({}).toArray();

      let metadata = collection.getQueryMetadata();
      expect(metadata?.hasDataLoss).toBe(false);

      // Inject corrupted file
      await storage.put('testdb/users_corrupt.parquet', new Uint8Array([0x00]));

      // Second query - with corruption
      await collection.find({}, { skipCorruptedFiles: true }).toArray();

      metadata = collection.getQueryMetadata();
      expect(metadata?.hasDataLoss).toBe(true);
      expect(metadata?.skippedCount).toBeGreaterThan(0);
    });

    it('should reset metadata for new query without corruption', async () => {
      const { collection, storage } = createTestCollection();

      await collection.insertOne({ _id: '1', name: 'Alice' });

      // First query with corruption
      await storage.put('testdb/users_bad.parquet', new Uint8Array([0x00]));
      await collection.find({}, { skipCorruptedFiles: true }).toArray();

      let metadata = collection.getQueryMetadata();
      expect(metadata?.hasDataLoss).toBe(true);

      // Remove corrupted file
      await storage.delete('testdb/users_bad.parquet');

      // New query without corruption
      await collection.find({}, { skipCorruptedFiles: true }).toArray();

      metadata = collection.getQueryMetadata();
      expect(metadata?.hasDataLoss).toBe(false);
      expect(metadata?.skippedCount).toBe(0);
      expect(metadata?.corruptedFiles).toHaveLength(0);
    });
  });

  describe('CorruptionReport interface', () => {
    it('should include all required fields', async () => {
      const { collection, storage } = createTestCollection('testcoll', 'testdb');

      await storage.put('testdb/testcoll_bad.parquet', new Uint8Array([0xDE, 0xAD]));

      await collection.find({}, { skipCorruptedFiles: true }).toArray();

      const metadata = collection.getQueryMetadata();
      const report = metadata?.corruptedFiles[0];

      expect(report).toBeDefined();
      expect(report?.filename).toContain('testcoll_bad.parquet');
      expect(report?.error).toBeTypeOf('string');
      expect(report?.timestamp).toBeInstanceOf(Date);
      expect(report?.collection).toBe('testcoll');
      expect(report?.database).toBe('testdb');
    });

    it('should capture meaningful error messages', async () => {
      const { collection, storage } = createTestCollection();

      // Create an invalid parquet file
      await storage.put('testdb/users_invalid.parquet', new Uint8Array([
        // Invalid magic bytes
        0x00, 0x00, 0x00, 0x00
      ]));

      await collection.find({}, { skipCorruptedFiles: true }).toArray();

      const metadata = collection.getQueryMetadata();
      const report = metadata?.corruptedFiles[0];

      expect(report?.error).toBeDefined();
      expect(report?.error.length).toBeGreaterThan(0);
    });
  });

  describe('findOne with corruption handling', () => {
    it('should work with findOne and skipCorruptedFiles', async () => {
      const { collection, storage } = createTestCollection();

      await collection.insertOne({ _id: '1', name: 'Alice' });
      await storage.put('testdb/users_bad.parquet', new Uint8Array([0x00]));

      const doc = await collection.findOne({ _id: '1' }, { skipCorruptedFiles: true });

      expect(doc).not.toBeNull();
      expect(doc?.name).toBe('Alice');

      const metadata = collection.getQueryMetadata();
      expect(metadata?.hasDataLoss).toBe(true);
    });
  });

  describe('edge cases', () => {
    it('should handle empty collection with corrupted files', async () => {
      const { collection, storage } = createTestCollection();

      // Only corrupted files, no valid data
      await storage.put('testdb/users_a.parquet', new Uint8Array([0x01]));
      await storage.put('testdb/users_b.parquet', new Uint8Array([0x02]));

      const docs = await collection.find({}, { skipCorruptedFiles: true }).toArray();

      expect(docs).toHaveLength(0);

      const metadata = collection.getQueryMetadata();
      expect(metadata?.hasDataLoss).toBe(true);
      expect(metadata?.skippedCount).toBe(2);
      expect(metadata?.corruptedFiles).toHaveLength(2);
    });

    it('should handle callback errors gracefully', async () => {
      const { collection, storage } = createTestCollection();

      await collection.insertOne({ _id: '1', name: 'Alice' });
      await storage.put('testdb/users_bad.parquet', new Uint8Array([0x00]));

      const errorCallback = vi.fn(() => {
        throw new Error('Callback error');
      });

      // Callback error should propagate
      await expect(
        collection.find({}, {
          skipCorruptedFiles: true,
          onCorruptedFile: errorCallback,
        }).toArray()
      ).rejects.toThrow('Callback error');
    });

    it('should include timestamp close to query time', async () => {
      const { collection, storage } = createTestCollection();

      await storage.put('testdb/users_bad.parquet', new Uint8Array([0x00]));

      const beforeQuery = new Date();
      await collection.find({}, { skipCorruptedFiles: true }).toArray();
      const afterQuery = new Date();

      const metadata = collection.getQueryMetadata();
      const reportTimestamp = metadata?.corruptedFiles[0]?.timestamp;

      expect(reportTimestamp).toBeDefined();
      expect(reportTimestamp!.getTime()).toBeGreaterThanOrEqual(beforeQuery.getTime());
      expect(reportTimestamp!.getTime()).toBeLessThanOrEqual(afterQuery.getTime());
    });
  });
});
