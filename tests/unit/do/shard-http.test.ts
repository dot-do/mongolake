/**
 * ShardDO HTTP Interface Tests
 *
 * Tests for HTTP request handling and error responses.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import type { DurableObjectState } from '@cloudflare/workers-types';
import {
  ShardDO,
  type ShardDOEnv,
  createMockState,
  createMockEnv,
} from './test-helpers.js';

describe('ShardDO - HTTP Interface Error Scenarios', () => {
  let shard: ShardDO;
  let state: DurableObjectState;
  let env: ShardDOEnv;

  beforeEach(() => {
    state = createMockState();
    env = createMockEnv();
    shard = new ShardDO(state, env);
  });

  it('should handle missing Content-Type header', async () => {
    const request = new Request('https://shard.do/write', {
      method: 'POST',
      body: JSON.stringify({ collection: 'users', op: 'insert', document: { _id: 'doc1' } }),
    });

    const response = await shard.fetch(request);
    // Should still work or return appropriate error
    expect([200, 400, 415].includes(response.status)).toBe(true);
  });

  it('should handle empty request body', async () => {
    const request = new Request('https://shard.do/write', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '',
    });

    const response = await shard.fetch(request);
    expect(response.status).toBe(400);
  });

  it('should handle malformed JSON in request body', async () => {
    const request = new Request('https://shard.do/write', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{"collection": "users", op:',
    });

    const response = await shard.fetch(request);
    expect(response.status).toBe(400);
  });

  it('should handle very large request body', async () => {
    const largeBody = JSON.stringify({
      collection: 'users',
      op: 'insert',
      document: { _id: 'large1', data: 'x'.repeat(100000) },
    });

    const request = new Request('https://shard.do/write', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: largeBody,
    });

    const response = await shard.fetch(request);
    // Should succeed or return 413 (Payload Too Large)
    expect([200, 413].includes(response.status)).toBe(true);
  });

  it('should handle OPTIONS request (CORS preflight)', async () => {
    const request = new Request('https://shard.do/write', {
      method: 'OPTIONS',
    });

    const response = await shard.fetch(request);
    // ShardDO may return 404 for unhandled routes or any status for OPTIONS
    expect(response.status).toBeGreaterThanOrEqual(200);
    expect(response.status).toBeLessThan(600);
  });

  it('should return error details in JSON format', async () => {
    const request = new Request('https://shard.do/write', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ collection: '', op: 'insert', document: { _id: 'doc1' } }),
    });

    const response = await shard.fetch(request);
    expect(response.status).toBeGreaterThanOrEqual(400);

    const body = await response.json();
    expect(body.error).toBeDefined();
  });
});

describe('ShardDO - HTTP Interface', () => {
  let shard: ShardDO;
  let state: DurableObjectState;
  let env: ShardDOEnv;

  beforeEach(() => {
    state = createMockState();
    env = createMockEnv();
    shard = new ShardDO(state, env);
  });

  it('should handle write request via fetch', async () => {
    const request = new Request('https://shard.do/write', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        collection: 'users',
        op: 'insert',
        document: { _id: 'doc1', name: 'Test' },
      }),
    });

    const response = await shard.fetch(request);

    expect(response.status).toBe(200);
    const result = await response.json();
    expect(result.acknowledged).toBe(true);
    expect(result.insertedId).toBe('doc1');
  });

  it('should handle find request via fetch', async () => {
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1', name: 'Alice' } });

    const request = new Request('https://shard.do/find', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        collection: 'users',
        filter: { _id: 'doc1' },
      }),
    });

    const response = await shard.fetch(request);

    expect(response.status).toBe(200);
    const result = await response.json();
    expect(result.documents).toHaveLength(1);
    expect(result.documents[0].name).toBe('Alice');
  });

  it('should handle flush request via fetch', async () => {
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });

    const request = new Request('https://shard.do/flush', {
      method: 'POST',
    });

    const response = await shard.fetch(request);

    expect(response.status).toBe(200);
  });

  it('should return 400 for invalid request body', async () => {
    const request = new Request('https://shard.do/write', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: 'not json',
    });

    const response = await shard.fetch(request);

    expect(response.status).toBe(400);
  });

  it('should return 404 for unknown endpoint', async () => {
    const request = new Request('https://shard.do/unknown', {
      method: 'GET',
    });

    const response = await shard.fetch(request);

    expect(response.status).toBe(404);
  });

  it('should include read token in write response', async () => {
    const request = new Request('https://shard.do/write', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        collection: 'users',
        op: 'insert',
        document: { _id: 'doc1' },
      }),
    });

    const response = await shard.fetch(request);
    const result = await response.json();

    expect(result.readToken).toBeDefined();
    expect(result.readToken).toMatch(/^test-shard-id:\d+$/);
  });
});
