/**
 * ShardDO Read Tokens Tests
 *
 * Tests for read tokens with shard:lsn format.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import type { DurableObjectState } from '@cloudflare/workers-types';
import {
  ShardDO,
  type ShardDOEnv,
  createMockState,
  createMockEnv,
} from './test-helpers.js';

describe('ShardDO - Read Tokens', () => {
  let shard: ShardDO;
  let state: DurableObjectState;
  let env: ShardDOEnv;

  beforeEach(() => {
    state = createMockState();
    env = createMockEnv();
    shard = new ShardDO(state, env);
  });

  it('should generate read token after write', async () => {
    const result = await shard.write({
      collection: 'users',
      op: 'insert',
      document: { _id: 'doc1' },
    });

    expect(result.readToken).toBeDefined();
    expect(typeof result.readToken).toBe('string');
  });

  it('should format read token as shard:lsn', async () => {
    const result = await shard.write({
      collection: 'users',
      op: 'insert',
      document: { _id: 'doc1' },
    });

    const token = result.readToken;
    const parts = token.split(':');
    expect(parts).toHaveLength(2);
    expect(parts[0]).toBe('test-shard-id');
    expect(parseInt(parts[1], 10)).toBeGreaterThan(0);
  });

  it('should generate increasing LSN in read tokens', async () => {
    const result1 = await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });
    const result2 = await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc2' } });

    const lsn1 = parseInt(result1.readToken.split(':')[1], 10);
    const lsn2 = parseInt(result2.readToken.split(':')[1], 10);

    expect(lsn2).toBeGreaterThan(lsn1);
  });

  it('should support read-after-write using read token', async () => {
    const writeResult = await shard.write({
      collection: 'users',
      op: 'insert',
      document: { _id: 'doc1', name: 'Test' },
    });

    const result = await shard.findOne('users', { _id: 'doc1' }, { afterToken: writeResult.readToken });

    expect(result).toBeDefined();
    expect(result?.name).toBe('Test');
  });

  it('should reject stale read token', async () => {
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc2' } });
    const result3 = await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc3' } });

    const futureToken = `test-shard-id:${result3.lsn + 1000}`;

    await expect(shard.findOne('users', { _id: 'doc1' }, { afterToken: futureToken })).rejects.toThrow();
  });

  it('should return current read token without write', async () => {
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });

    const token = await shard.getCurrentReadToken();

    expect(token).toBeDefined();
    expect(token).toMatch(/^test-shard-id:\d+$/);
  });

  it('should parse read token correctly', () => {
    const token = 'shard-abc123:42';
    const parsed = ShardDO.parseReadToken(token);

    expect(parsed.shardId).toBe('shard-abc123');
    expect(parsed.lsn).toBe(42);
  });

  it('should throw on invalid read token format', () => {
    expect(() => ShardDO.parseReadToken('invalid')).toThrow();
    expect(() => ShardDO.parseReadToken('shard:abc')).toThrow();
    expect(() => ShardDO.parseReadToken('')).toThrow();
  });

  it('should reject read token from different shard', async () => {
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });

    const wrongShardToken = 'different-shard-id:1';

    await expect(
      shard.findOne('users', { _id: 'doc1' }, { afterToken: wrongShardToken })
    ).rejects.toThrow(/shard ID mismatch/);
  });

  it('should accept valid read token with matching shard ID', async () => {
    const writeResult = await shard.write({
      collection: 'users',
      op: 'insert',
      document: { _id: 'doc1', name: 'Test' },
    });

    const parsedToken = ShardDO.parseReadToken(writeResult.readToken);
    expect(parsedToken.shardId).toBe('test-shard-id');

    const result = await shard.findOne('users', { _id: 'doc1' }, { afterToken: writeResult.readToken });
    expect(result?.name).toBe('Test');
  });

  it('should throw on read token with future LSN and correct shard ID', async () => {
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });

    const futureToken = 'test-shard-id:99999';

    await expect(
      shard.findOne('users', { _id: 'doc1' }, { afterToken: futureToken })
    ).rejects.toThrow(/future LSN/);
  });

  it('should accept read token with LSN equal to current LSN', async () => {
    const writeResult = await shard.write({
      collection: 'users',
      op: 'insert',
      document: { _id: 'doc1', name: 'Test' },
    });

    const result = await shard.findOne('users', { _id: 'doc1' }, { afterToken: writeResult.readToken });
    expect(result?.name).toBe('Test');
  });

  it('should accept read token with LSN less than current LSN', async () => {
    const writeResult1 = await shard.write({
      collection: 'users',
      op: 'insert',
      document: { _id: 'doc1', name: 'First' },
    });

    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc2', name: 'Second' } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc3', name: 'Third' } });

    const result = await shard.findOne('users', { _id: 'doc1' }, { afterToken: writeResult1.readToken });
    expect(result?.name).toBe('First');
  });

  it('should throw descriptive error for empty read token', () => {
    expect(() => ShardDO.parseReadToken('')).toThrow(/cannot be empty/);
    expect(() => ShardDO.parseReadToken('   ')).toThrow(/cannot be empty/);
  });

  it('should throw descriptive error for malformed read token', () => {
    expect(() => ShardDO.parseReadToken('no-colon-here')).toThrow(/format invalid/);
    expect(() => ShardDO.parseReadToken('too:many:colons')).toThrow(/format invalid/);
  });

  it('should throw descriptive error for non-numeric LSN', () => {
    expect(() => ShardDO.parseReadToken('shard:notanumber')).toThrow(/not a valid number/);
    const parsed = ShardDO.parseReadToken('shard:12.34');
    expect(parsed.lsn).toBe(12);
  });

  it('should parse read token with negative LSN (edge case)', () => {
    const parsed = ShardDO.parseReadToken('shard:-1');
    expect(parsed.shardId).toBe('shard');
    expect(parsed.lsn).toBe(-1);
  });

  it('should parse read token with zero LSN', () => {
    const parsed = ShardDO.parseReadToken('shard:0');
    expect(parsed.shardId).toBe('shard');
    expect(parsed.lsn).toBe(0);
  });
});
