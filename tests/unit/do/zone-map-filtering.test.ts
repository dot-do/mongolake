/**
 * Zone Map Filtering Tests
 *
 * Tests for zone map-based predicate pushdown in query execution.
 * Zone maps enable skipping files that cannot contain matching documents.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import type { DurableObjectState, R2Bucket } from '@cloudflare/workers-types';
import {
  ShardDO,
  type ShardDOEnv,
  createMockState,
  createMockR2Bucket,
  createMockEnv,
} from './test-helpers.js';
import {
  extractZoneMapPredicates,
  canFileMatchFilter,
  generateZoneMapEntries,
} from '../../../src/utils/zone-map-filter.js';
import type { FileZoneMapEntry } from '../../../src/do/shard/types.js';

// ============================================================================
// Zone Map Predicate Extraction Tests
// ============================================================================

describe('extractZoneMapPredicates', () => {
  it('should extract simple equality predicates', () => {
    const filter = { name: 'Alice', age: 30 };
    const predicates = extractZoneMapPredicates(filter);

    expect(predicates).toHaveLength(2);
    expect(predicates).toContainEqual({ field: 'name', op: '$eq', value: 'Alice' });
    expect(predicates).toContainEqual({ field: 'age', op: '$eq', value: 30 });
  });

  it('should extract comparison operators', () => {
    const filter = {
      age: { $gt: 18, $lte: 65 },
      score: { $gte: 50 },
    };
    const predicates = extractZoneMapPredicates(filter);

    expect(predicates).toHaveLength(3);
    expect(predicates).toContainEqual({ field: 'age', op: '$gt', value: 18 });
    expect(predicates).toContainEqual({ field: 'age', op: '$lte', value: 65 });
    expect(predicates).toContainEqual({ field: 'score', op: '$gte', value: 50 });
  });

  it('should extract $in operator', () => {
    const filter = { status: { $in: ['active', 'pending'] } };
    const predicates = extractZoneMapPredicates(filter);

    expect(predicates).toHaveLength(1);
    expect(predicates[0]).toEqual({
      field: 'status',
      op: '$in',
      value: ['active', 'pending'],
    });
  });

  it('should extract $ne operator', () => {
    const filter = { status: { $ne: 'deleted' } };
    const predicates = extractZoneMapPredicates(filter);

    expect(predicates).toHaveLength(1);
    expect(predicates[0]).toEqual({ field: 'status', op: '$ne', value: 'deleted' });
  });

  it('should skip $and/$or/$nor operators', () => {
    const filter = {
      $and: [{ age: { $gt: 18 } }, { status: 'active' }],
      name: 'Alice',
    };
    const predicates = extractZoneMapPredicates(filter);

    // Only extracts the direct field predicate, not the $and contents
    expect(predicates).toHaveLength(1);
    expect(predicates[0]).toEqual({ field: 'name', op: '$eq', value: 'Alice' });
  });

  it('should handle null value predicate', () => {
    const filter = { middleName: null };
    const predicates = extractZoneMapPredicates(filter);

    expect(predicates).toHaveLength(1);
    expect(predicates[0]).toEqual({ field: 'middleName', op: '$eq', value: null });
  });
});

// ============================================================================
// Zone Map Entry Generation Tests
// ============================================================================

describe('generateZoneMapEntries', () => {
  it('should generate entries for numeric fields', () => {
    const docs = [
      { _id: '1', age: 25, score: 80 },
      { _id: '2', age: 30, score: 90 },
      { _id: '3', age: 35, score: 70 },
    ];
    const entries = generateZoneMapEntries(docs);

    const ageEntry = entries.find((e) => e.field === 'age');
    expect(ageEntry).toBeDefined();
    expect(ageEntry!.min).toBe(25);
    expect(ageEntry!.max).toBe(35);
    expect(ageEntry!.nullCount).toBe(0);
    expect(ageEntry!.rowCount).toBe(3);
  });

  it('should generate entries for string fields', () => {
    const docs = [
      { _id: '1', name: 'Charlie' },
      { _id: '2', name: 'Alice' },
      { _id: '3', name: 'Bob' },
    ];
    const entries = generateZoneMapEntries(docs);

    const nameEntry = entries.find((e) => e.field === 'name');
    expect(nameEntry).toBeDefined();
    expect(nameEntry!.min).toBe('Alice');
    expect(nameEntry!.max).toBe('Charlie');
  });

  it('should track null counts', () => {
    const docs = [
      { _id: '1', value: 10 },
      { _id: '2', value: null },
      { _id: '3', value: 20 },
      { _id: '4', value: undefined }, // explicit undefined counts as null
    ];
    const entries = generateZoneMapEntries(docs);

    const valueEntry = entries.find((e) => e.field === 'value');
    expect(valueEntry).toBeDefined();
    expect(valueEntry!.min).toBe(10);
    expect(valueEntry!.max).toBe(20);
    expect(valueEntry!.nullCount).toBe(2); // null and undefined
  });

  it('should not count missing fields as null (sparse fields)', () => {
    // Documents without a field are not counted in nullCount
    // The field is only tracked when it appears in at least one document
    const docs = [
      { _id: '1', value: 10 },
      { _id: '2', value: 20 },
      { _id: '3' }, // no value field at all - not counted
    ];
    const entries = generateZoneMapEntries(docs);

    const valueEntry = entries.find((e) => e.field === 'value');
    expect(valueEntry).toBeDefined();
    expect(valueEntry!.nullCount).toBe(0); // missing fields not counted
    // rowCount represents total documents in the batch (used for file metadata)
    expect(valueEntry!.rowCount).toBe(3);
  });

  it('should prioritize _id and _seq fields', () => {
    const docs = [
      { _id: 'doc1', _seq: 1, a: 1, b: 2, c: 3 },
      { _id: 'doc2', _seq: 2, a: 4, b: 5, c: 6 },
    ];
    const entries = generateZoneMapEntries(docs);

    // _id and _seq should be first
    expect(entries[0].field).toBe('_id');
    expect(entries[1].field).toBe('_seq');
  });

  it('should skip complex object fields', () => {
    const docs = [
      { _id: '1', data: { nested: true }, value: 10 },
      { _id: '2', data: { nested: false }, value: 20 },
    ];
    const entries = generateZoneMapEntries(docs);

    // Should not include 'data' field
    const dataEntry = entries.find((e) => e.field === 'data');
    expect(dataEntry).toBeUndefined();

    // Should include 'value' field
    const valueEntry = entries.find((e) => e.field === 'value');
    expect(valueEntry).toBeDefined();
  });

  it('should handle empty document array', () => {
    const entries = generateZoneMapEntries([]);
    expect(entries).toHaveLength(0);
  });

  it('should limit number of tracked fields', () => {
    // Create docs with many fields
    const doc: Record<string, unknown> = { _id: '1' };
    for (let i = 0; i < 50; i++) {
      doc[`field${i}`] = i;
    }
    const entries = generateZoneMapEntries([doc], 5);

    expect(entries.length).toBeLessThanOrEqual(5);
  });
});

// ============================================================================
// canFileMatchFilter Tests
// ============================================================================

describe('canFileMatchFilter', () => {
  it('should return true when zone map is undefined', () => {
    const result = canFileMatchFilter(undefined, { age: 30 });
    expect(result).toBe(true);
  });

  it('should return true when zone map is empty', () => {
    const result = canFileMatchFilter([], { age: 30 });
    expect(result).toBe(true);
  });

  it('should return true when filter is empty', () => {
    const zoneMap: FileZoneMapEntry[] = [
      { field: 'age', min: 20, max: 40, nullCount: 0, rowCount: 10 },
    ];
    const result = canFileMatchFilter(zoneMap, {});
    expect(result).toBe(true);
  });

  describe('$eq operator', () => {
    const zoneMap: FileZoneMapEntry[] = [
      { field: 'age', min: 20, max: 40, nullCount: 0, rowCount: 10 },
    ];

    it('should return true when value is in range', () => {
      expect(canFileMatchFilter(zoneMap, { age: 30 })).toBe(true);
    });

    it('should return true when value equals min', () => {
      expect(canFileMatchFilter(zoneMap, { age: 20 })).toBe(true);
    });

    it('should return true when value equals max', () => {
      expect(canFileMatchFilter(zoneMap, { age: 40 })).toBe(true);
    });

    it('should return false when value is below range', () => {
      expect(canFileMatchFilter(zoneMap, { age: 10 })).toBe(false);
    });

    it('should return false when value is above range', () => {
      expect(canFileMatchFilter(zoneMap, { age: 50 })).toBe(false);
    });
  });

  describe('$gt operator', () => {
    const zoneMap: FileZoneMapEntry[] = [
      { field: 'age', min: 20, max: 40, nullCount: 0, rowCount: 10 },
    ];

    it('should return true when max > value', () => {
      expect(canFileMatchFilter(zoneMap, { age: { $gt: 30 } })).toBe(true);
    });

    it('should return false when max <= value', () => {
      expect(canFileMatchFilter(zoneMap, { age: { $gt: 40 } })).toBe(false);
      expect(canFileMatchFilter(zoneMap, { age: { $gt: 50 } })).toBe(false);
    });
  });

  describe('$gte operator', () => {
    const zoneMap: FileZoneMapEntry[] = [
      { field: 'age', min: 20, max: 40, nullCount: 0, rowCount: 10 },
    ];

    it('should return true when max >= value', () => {
      expect(canFileMatchFilter(zoneMap, { age: { $gte: 40 } })).toBe(true);
      expect(canFileMatchFilter(zoneMap, { age: { $gte: 30 } })).toBe(true);
    });

    it('should return false when max < value', () => {
      expect(canFileMatchFilter(zoneMap, { age: { $gte: 50 } })).toBe(false);
    });
  });

  describe('$lt operator', () => {
    const zoneMap: FileZoneMapEntry[] = [
      { field: 'age', min: 20, max: 40, nullCount: 0, rowCount: 10 },
    ];

    it('should return true when min < value', () => {
      expect(canFileMatchFilter(zoneMap, { age: { $lt: 30 } })).toBe(true);
    });

    it('should return false when min >= value', () => {
      expect(canFileMatchFilter(zoneMap, { age: { $lt: 20 } })).toBe(false);
      expect(canFileMatchFilter(zoneMap, { age: { $lt: 10 } })).toBe(false);
    });
  });

  describe('$lte operator', () => {
    const zoneMap: FileZoneMapEntry[] = [
      { field: 'age', min: 20, max: 40, nullCount: 0, rowCount: 10 },
    ];

    it('should return true when min <= value', () => {
      expect(canFileMatchFilter(zoneMap, { age: { $lte: 20 } })).toBe(true);
      expect(canFileMatchFilter(zoneMap, { age: { $lte: 30 } })).toBe(true);
    });

    it('should return false when min > value', () => {
      expect(canFileMatchFilter(zoneMap, { age: { $lte: 10 } })).toBe(false);
    });
  });

  describe('$in operator', () => {
    const zoneMap: FileZoneMapEntry[] = [
      { field: 'age', min: 20, max: 40, nullCount: 0, rowCount: 10 },
    ];

    it('should return true when any value in set is in range', () => {
      expect(canFileMatchFilter(zoneMap, { age: { $in: [10, 30, 50] } })).toBe(true);
    });

    it('should return false when no values in set are in range', () => {
      expect(canFileMatchFilter(zoneMap, { age: { $in: [10, 15, 50, 60] } })).toBe(false);
    });
  });

  describe('$ne operator', () => {
    it('should return false when all values equal the excluded value', () => {
      const zoneMap: FileZoneMapEntry[] = [
        { field: 'status', min: 'active', max: 'active', nullCount: 0, rowCount: 10 },
      ];
      expect(canFileMatchFilter(zoneMap, { status: { $ne: 'active' } })).toBe(false);
    });

    it('should return true when range has multiple values', () => {
      const zoneMap: FileZoneMapEntry[] = [
        { field: 'status', min: 'active', max: 'pending', nullCount: 0, rowCount: 10 },
      ];
      expect(canFileMatchFilter(zoneMap, { status: { $ne: 'active' } })).toBe(true);
    });
  });

  describe('null value handling', () => {
    it('should match null predicate when file has nulls', () => {
      const zoneMap: FileZoneMapEntry[] = [
        { field: 'value', min: 10, max: 20, nullCount: 5, rowCount: 10 },
      ];
      expect(canFileMatchFilter(zoneMap, { value: null })).toBe(true);
    });

    it('should not match null predicate when file has no nulls', () => {
      const zoneMap: FileZoneMapEntry[] = [
        { field: 'value', min: 10, max: 20, nullCount: 0, rowCount: 10 },
      ];
      expect(canFileMatchFilter(zoneMap, { value: null })).toBe(false);
    });
  });

  describe('string fields', () => {
    const zoneMap: FileZoneMapEntry[] = [
      { field: 'name', min: 'Alice', max: 'Charlie', nullCount: 0, rowCount: 10 },
    ];

    it('should match string in range', () => {
      expect(canFileMatchFilter(zoneMap, { name: 'Bob' })).toBe(true);
    });

    it('should not match string below range', () => {
      expect(canFileMatchFilter(zoneMap, { name: 'Aaron' })).toBe(false);
    });

    it('should not match string above range', () => {
      expect(canFileMatchFilter(zoneMap, { name: 'David' })).toBe(false);
    });
  });

  describe('multiple predicates', () => {
    const zoneMap: FileZoneMapEntry[] = [
      { field: 'age', min: 20, max: 40, nullCount: 0, rowCount: 10 },
      { field: 'score', min: 50, max: 100, nullCount: 0, rowCount: 10 },
    ];

    it('should return true when all predicates may match', () => {
      expect(canFileMatchFilter(zoneMap, { age: 30, score: 75 })).toBe(true);
    });

    it('should return false when any predicate definitely does not match', () => {
      expect(canFileMatchFilter(zoneMap, { age: 30, score: 200 })).toBe(false);
      expect(canFileMatchFilter(zoneMap, { age: 50, score: 75 })).toBe(false);
    });
  });

  describe('field not in zone map', () => {
    const zoneMap: FileZoneMapEntry[] = [
      { field: 'age', min: 20, max: 40, nullCount: 0, rowCount: 10 },
    ];

    it('should return true when filtering on untracked field', () => {
      expect(canFileMatchFilter(zoneMap, { status: 'active' })).toBe(true);
    });
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('Zone Map Filtering Integration', () => {
  let shard: ShardDO;
  let state: DurableObjectState;
  let env: ShardDOEnv;
  let bucket: R2Bucket;

  beforeEach(async () => {
    state = createMockState();
    bucket = createMockR2Bucket();
    env = createMockEnv(bucket);
    shard = new ShardDO(state, env);
  });

  it('should include zone map in file metadata after flush', async () => {
    // Insert documents with varying ages
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'u1', age: 25 } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'u2', age: 30 } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'u3', age: 35 } });

    // Flush to R2
    await shard.flush();

    // Get the manifest to check zone map
    const manifest = await shard.getManifest('users');
    expect(manifest.files).toHaveLength(1);

    const file = manifest.files[0];
    expect(file.zoneMap).toBeDefined();
    expect(file.zoneMap!.length).toBeGreaterThan(0);

    // Verify age field is tracked
    const ageEntry = file.zoneMap!.find((e) => e.field === 'age');
    expect(ageEntry).toBeDefined();
    expect(ageEntry!.min).toBe(25);
    expect(ageEntry!.max).toBe(35);
  });

  it('should skip files that cannot match filter', async () => {
    // Create two files with non-overlapping age ranges
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'young1', age: 20 } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'young2', age: 25 } });
    await shard.flush();

    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'old1', age: 60 } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'old2', age: 65 } });
    await shard.flush();

    // Verify we have 2 files
    const manifest = await shard.getManifest('users');
    expect(manifest.files).toHaveLength(2);

    // Query for young users (should skip old users file)
    const results = await shard.find('users', { age: { $lt: 30 } });
    expect(results).toHaveLength(2);
    expect(results.map((r) => r._id)).toEqual(expect.arrayContaining(['young1', 'young2']));

    // Query for old users (should skip young users file)
    const oldResults = await shard.find('users', { age: { $gt: 50 } });
    expect(oldResults).toHaveLength(2);
    expect(oldResults.map((r) => r._id)).toEqual(expect.arrayContaining(['old1', 'old2']));
  });

  it('should correctly handle queries across multiple files', async () => {
    // Create multiple files
    for (let batch = 0; batch < 3; batch++) {
      for (let i = 0; i < 3; i++) {
        const age = batch * 30 + i * 10 + 10; // 10-30, 40-60, 70-90
        await shard.write({
          collection: 'users',
          op: 'insert',
          document: { _id: `user-${batch}-${i}`, age },
        });
      }
      await shard.flush();
    }

    // Query spanning multiple files
    const results = await shard.find('users', { age: { $gte: 30, $lte: 60 } });
    const ages = results.map((r) => r.age as number);

    // Should include ages 30, 40, 50, 60
    for (const age of ages) {
      expect(age).toBeGreaterThanOrEqual(30);
      expect(age).toBeLessThanOrEqual(60);
    }
  });

  it('should handle string field zone maps', async () => {
    await shard.write({ collection: 'users', op: 'insert', document: { _id: '1', name: 'Alice' } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: '2', name: 'Bob' } });
    await shard.flush();

    await shard.write({ collection: 'users', op: 'insert', document: { _id: '3', name: 'Zack' } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: '4', name: 'Yolanda' } });
    await shard.flush();

    // Query for names starting with early letters (should skip second file)
    const results = await shard.find('users', { name: { $lt: 'C' } });
    expect(results).toHaveLength(2);
    expect(results.map((r) => r.name)).toEqual(expect.arrayContaining(['Alice', 'Bob']));
  });

  it('should fall back to full scan for untracked fields', async () => {
    await shard.write({
      collection: 'users',
      op: 'insert',
      document: { _id: '1', name: 'Alice', hobby: 'reading' },
    });
    await shard.write({
      collection: 'users',
      op: 'insert',
      document: { _id: '2', name: 'Bob', hobby: 'gaming' },
    });
    await shard.flush();

    // hobby might not be in zone map if there are too many fields
    // Should still return correct results via full scan
    const results = await shard.find('users', { hobby: 'reading' });
    expect(results).toHaveLength(1);
    expect(results[0].name).toBe('Alice');
  });
});
