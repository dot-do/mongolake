/**
 * ShardDO Query Tests
 *
 * Tests for querying buffered documents and merging results.
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

describe('ShardDO - Query Buffered Documents', () => {
  let shard: ShardDO;
  let state: DurableObjectState;
  let env: ShardDOEnv;

  beforeEach(async () => {
    state = createMockState();
    env = createMockEnv();
    shard = new ShardDO(state, env);

    // Pre-populate with test data
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'alice', name: 'Alice', age: 30 } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'bob', name: 'Bob', age: 25 } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'charlie', name: 'Charlie', age: 35 } });
  });

  it('should find document by _id in buffer', async () => {
    const result = await shard.findOne('users', { _id: 'alice' });

    expect(result).toBeDefined();
    expect(result?._id).toBe('alice');
    expect(result?.name).toBe('Alice');
  });

  it('should find documents by field value in buffer', async () => {
    const results = await shard.find('users', { age: 30 });

    expect(results).toHaveLength(1);
    expect(results[0].name).toBe('Alice');
  });

  it('should find documents with comparison operators', async () => {
    const results = await shard.find('users', { age: { $gt: 28 } });

    expect(results).toHaveLength(2);
    const names = results.map((r) => r.name);
    expect(names).toContain('Alice');
    expect(names).toContain('Charlie');
  });

  it('should return empty array when no documents match', async () => {
    const results = await shard.find('users', { age: 100 });

    expect(results).toHaveLength(0);
  });

  it('should return null when findOne has no match', async () => {
    const result = await shard.findOne('users', { _id: 'nonexistent' });

    expect(result).toBeNull();
  });

  it('should reflect updates to buffered documents', async () => {
    await shard.write({
      collection: 'users',
      op: 'update',
      filter: { _id: 'alice' },
      update: { $set: { age: 31 } },
    });

    const result = await shard.findOne('users', { _id: 'alice' });
    expect(result?.age).toBe(31);
  });

  it('should not return deleted documents', async () => {
    await shard.write({
      collection: 'users',
      op: 'delete',
      filter: { _id: 'bob' },
    });

    const result = await shard.findOne('users', { _id: 'bob' });
    expect(result).toBeNull();

    const allUsers = await shard.find('users', {});
    expect(allUsers).toHaveLength(2);
  });

  it('should support projection', async () => {
    const result = await shard.findOne('users', { _id: 'alice' }, { projection: { name: 1 } });

    expect(result).toBeDefined();
    expect(result?.name).toBe('Alice');
    expect(result?.age).toBeUndefined();
  });

  it('should support sorting', async () => {
    const results = await shard.find('users', {}, { sort: { age: 1 } });

    expect(results[0].name).toBe('Bob'); // age 25
    expect(results[1].name).toBe('Alice'); // age 30
    expect(results[2].name).toBe('Charlie'); // age 35
  });

  it('should support limit', async () => {
    const results = await shard.find('users', {}, { limit: 2 });

    expect(results).toHaveLength(2);
  });
});

describe('ShardDO - Merge Query Results', () => {
  let shard: ShardDO;
  let state: DurableObjectState;
  let env: ShardDOEnv;
  let bucket: R2Bucket;

  beforeEach(async () => {
    state = createMockState();
    bucket = createMockR2Bucket();
    env = createMockEnv(bucket);
    shard = new ShardDO(state, env);

    // Flush some documents to R2
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'old1', name: 'Old1', age: 40 } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'old2', name: 'Old2', age: 45 } });
    await shard.flush();

    // Add new documents to buffer
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'new1', name: 'New1', age: 25 } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'new2', name: 'New2', age: 30 } });
  });

  it('should return documents from both buffer and R2', async () => {
    const results = await shard.find('users', {});

    expect(results).toHaveLength(4);
    const ids = results.map((r) => r._id);
    expect(ids).toContain('old1');
    expect(ids).toContain('old2');
    expect(ids).toContain('new1');
    expect(ids).toContain('new2');
  });

  it('should apply filter across both buffer and R2', async () => {
    const results = await shard.find('users', { age: { $gte: 30 } });

    expect(results).toHaveLength(3);
    const ids = results.map((r) => r._id);
    expect(ids).toContain('old1'); // age 40
    expect(ids).toContain('old2'); // age 45
    expect(ids).toContain('new2'); // age 30
    expect(ids).not.toContain('new1'); // age 25
  });

  it('should prefer buffer version when document exists in both', async () => {
    await shard.write({
      collection: 'users',
      op: 'update',
      filter: { _id: 'old1' },
      update: { $set: { name: 'Updated' } },
    });

    const result = await shard.findOne('users', { _id: 'old1' });
    expect(result?.name).toBe('Updated');
  });

  it('should respect deletion in buffer over R2 version', async () => {
    await shard.write({
      collection: 'users',
      op: 'delete',
      filter: { _id: 'old1' },
    });

    const result = await shard.findOne('users', { _id: 'old1' });
    expect(result).toBeNull();

    const allResults = await shard.find('users', {});
    expect(allResults).toHaveLength(3);
  });

  it('should maintain sort order across merged results', async () => {
    const results = await shard.find('users', {}, { sort: { age: 1 } });

    const ages = results.map((r) => r.age);
    expect(ages).toEqual([25, 30, 40, 45]);
  });

  it('should apply limit after merging', async () => {
    const results = await shard.find('users', {}, { sort: { age: 1 }, limit: 2 });

    expect(results).toHaveLength(2);
    expect(results[0].age).toBe(25);
    expect(results[1].age).toBe(30);
  });

  it('should skip correctly across merged results', async () => {
    const results = await shard.find('users', {}, { sort: { age: 1 }, skip: 2 });

    expect(results).toHaveLength(2);
    expect(results[0].age).toBe(40);
    expect(results[1].age).toBe(45);
  });
});
