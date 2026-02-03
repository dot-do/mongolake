/**
 * ReplicaBuffer Tests
 *
 * Tests for the replica in-memory document buffer.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { ReplicaBuffer } from '../../../src/do/shard/replica-buffer.js';
import type { ReplicationWalEntry } from '../../../src/do/shard/replica-types.js';

describe('ReplicaBuffer', () => {
  let buffer: ReplicaBuffer;

  beforeEach(() => {
    buffer = new ReplicaBuffer({ cacheTtlMs: 5000 });
  });

  describe('applyEntry', () => {
    it('should add a document on insert', () => {
      const entry: ReplicationWalEntry = {
        lsn: 1,
        collection: 'users',
        op: 'i',
        docId: 'doc1',
        document: { name: 'Alice', age: 30 },
        timestamp: Date.now(),
      };

      buffer.applyEntry(entry);

      const doc = buffer.get('users', 'doc1');
      expect(doc).toBeDefined();
      expect(doc?.name).toBe('Alice');
      expect(doc?.age).toBe(30);
      expect(doc?._id).toBe('doc1');
    });

    it('should update a document on update', () => {
      // Insert first
      buffer.applyEntry({
        lsn: 1,
        collection: 'users',
        op: 'i',
        docId: 'doc1',
        document: { name: 'Alice', age: 30 },
        timestamp: Date.now(),
      });

      // Update
      buffer.applyEntry({
        lsn: 2,
        collection: 'users',
        op: 'u',
        docId: 'doc1',
        document: { name: 'Alice', age: 31 },
        timestamp: Date.now(),
      });

      const doc = buffer.get('users', 'doc1');
      expect(doc?.age).toBe(31);
    });

    it('should remove a document on delete', () => {
      // Insert first
      buffer.applyEntry({
        lsn: 1,
        collection: 'users',
        op: 'i',
        docId: 'doc1',
        document: { name: 'Alice' },
        timestamp: Date.now(),
      });

      expect(buffer.get('users', 'doc1')).toBeDefined();

      // Delete
      buffer.applyEntry({
        lsn: 2,
        collection: 'users',
        op: 'd',
        docId: 'doc1',
        document: {},
        timestamp: Date.now(),
      });

      expect(buffer.get('users', 'doc1')).toBeNull();
    });

    it('should track LSN correctly', () => {
      buffer.applyEntry({
        lsn: 5,
        collection: 'users',
        op: 'i',
        docId: 'doc1',
        document: { name: 'Alice' },
        timestamp: Date.now(),
      });

      expect(buffer.getCurrentLSN()).toBe(5);

      buffer.applyEntry({
        lsn: 10,
        collection: 'users',
        op: 'i',
        docId: 'doc2',
        document: { name: 'Bob' },
        timestamp: Date.now(),
      });

      expect(buffer.getCurrentLSN()).toBe(10);
    });

    it('should handle multiple collections', () => {
      buffer.applyEntry({
        lsn: 1,
        collection: 'users',
        op: 'i',
        docId: 'u1',
        document: { name: 'Alice' },
        timestamp: Date.now(),
      });

      buffer.applyEntry({
        lsn: 2,
        collection: 'orders',
        op: 'i',
        docId: 'o1',
        document: { item: 'Book' },
        timestamp: Date.now(),
      });

      expect(buffer.get('users', 'u1')?.name).toBe('Alice');
      expect(buffer.get('orders', 'o1')?.item).toBe('Book');
    });
  });

  describe('getAll', () => {
    it('should return all documents in a collection', () => {
      buffer.applyEntry({
        lsn: 1,
        collection: 'users',
        op: 'i',
        docId: 'doc1',
        document: { name: 'Alice' },
        timestamp: Date.now(),
      });

      buffer.applyEntry({
        lsn: 2,
        collection: 'users',
        op: 'i',
        docId: 'doc2',
        document: { name: 'Bob' },
        timestamp: Date.now(),
      });

      const docs = buffer.getAll('users');
      expect(docs.length).toBe(2);
      expect(docs.map(d => d.name).sort()).toEqual(['Alice', 'Bob']);
    });

    it('should return empty array for unknown collection', () => {
      const docs = buffer.getAll('unknown');
      expect(docs).toEqual([]);
    });
  });

  describe('has', () => {
    it('should return true for existing document', () => {
      buffer.applyEntry({
        lsn: 1,
        collection: 'users',
        op: 'i',
        docId: 'doc1',
        document: { name: 'Alice' },
        timestamp: Date.now(),
      });

      expect(buffer.has('users', 'doc1')).toBe(true);
    });

    it('should return false for non-existing document', () => {
      expect(buffer.has('users', 'doc1')).toBe(false);
    });
  });

  describe('getStats', () => {
    it('should return accurate statistics', () => {
      buffer.applyEntry({
        lsn: 1,
        collection: 'users',
        op: 'i',
        docId: 'u1',
        document: { name: 'Alice' },
        timestamp: Date.now(),
      });

      buffer.applyEntry({
        lsn: 2,
        collection: 'orders',
        op: 'i',
        docId: 'o1',
        document: { item: 'Book' },
        timestamp: Date.now(),
      });

      buffer.applyEntry({
        lsn: 3,
        collection: 'orders',
        op: 'i',
        docId: 'o2',
        document: { item: 'Pen' },
        timestamp: Date.now(),
      });

      const stats = buffer.getStats();
      expect(stats.collections).toBe(2);
      expect(stats.totalDocuments).toBe(3);
      expect(stats.currentLSN).toBe(3);
      expect(stats.perCollectionCounts['users']).toBe(1);
      expect(stats.perCollectionCounts['orders']).toBe(2);
    });
  });

  describe('clear', () => {
    it('should clear all data', () => {
      buffer.applyEntry({
        lsn: 1,
        collection: 'users',
        op: 'i',
        docId: 'doc1',
        document: { name: 'Alice' },
        timestamp: Date.now(),
      });

      buffer.clear();

      expect(buffer.get('users', 'doc1')).toBeNull();
      expect(buffer.getCurrentLSN()).toBe(0);
      expect(buffer.getStats().totalDocuments).toBe(0);
    });
  });

  describe('clearCollection', () => {
    it('should clear only specified collection', () => {
      buffer.applyEntry({
        lsn: 1,
        collection: 'users',
        op: 'i',
        docId: 'u1',
        document: { name: 'Alice' },
        timestamp: Date.now(),
      });

      buffer.applyEntry({
        lsn: 2,
        collection: 'orders',
        op: 'i',
        docId: 'o1',
        document: { item: 'Book' },
        timestamp: Date.now(),
      });

      buffer.clearCollection('users');

      expect(buffer.get('users', 'u1')).toBeNull();
      expect(buffer.get('orders', 'o1')?.item).toBe('Book');
    });
  });
});
