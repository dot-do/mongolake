/**
 * ObjectId Tests
 *
 * Tests for ObjectId generation and validation.
 */

import { describe, it, expect } from 'vitest';
import { ObjectId } from '../../../src/client/index.js';

describe('ObjectId', () => {
  describe('constructor', () => {
    it('should generate a new ObjectId when called without arguments', () => {
      const id = new ObjectId();
      expect(id.toString()).toHaveLength(24);
      expect(id.toHexString()).toHaveLength(24);
    });

    it('should create ObjectId from hex string', () => {
      const hex = '507f1f77bcf86cd799439011';
      const id = new ObjectId(hex);
      expect(id.toString()).toBe(hex);
    });

    it('should create ObjectId from Uint8Array', () => {
      const bytes = new Uint8Array(12);
      bytes.fill(0xab);
      const id = new ObjectId(bytes);
      expect(id.toString()).toBe('abababababababababababab');
    });

    it('should generate unique ObjectIds', () => {
      const ids = new Set<string>();
      for (let i = 0; i < 100; i++) {
        ids.add(new ObjectId().toString());
      }
      expect(ids.size).toBe(100);
    });
  });

  describe('toString/toHexString', () => {
    it('should return consistent hex string', () => {
      const id = new ObjectId();
      expect(id.toString()).toBe(id.toHexString());
    });

    it('should return lowercase hex string', () => {
      const id = new ObjectId('507F1F77BCF86CD799439011');
      expect(id.toString()).toMatch(/^[0-9a-f]{24}$/);
    });
  });

  describe('getTimestamp', () => {
    it('should return a valid Date', () => {
      const before = Math.floor(Date.now() / 1000);
      const id = new ObjectId();
      const after = Math.floor(Date.now() / 1000);

      const timestamp = id.getTimestamp();
      const seconds = Math.floor(timestamp.getTime() / 1000);

      expect(seconds).toBeGreaterThanOrEqual(before);
      expect(seconds).toBeLessThanOrEqual(after);
    });

    it('should extract timestamp from existing ObjectId', () => {
      // ObjectId from a known timestamp (2012-10-15 21:26:17 UTC)
      const id = new ObjectId('507f1f77bcf86cd799439011');
      const timestamp = id.getTimestamp();
      expect(timestamp.getFullYear()).toBe(2012);
    });
  });

  describe('equals', () => {
    it('should return true for identical ObjectIds', () => {
      const hex = '507f1f77bcf86cd799439011';
      const id1 = new ObjectId(hex);
      const id2 = new ObjectId(hex);
      expect(id1.equals(id2)).toBe(true);
    });

    it('should return false for different ObjectIds', () => {
      const id1 = new ObjectId();
      const id2 = new ObjectId();
      expect(id1.equals(id2)).toBe(false);
    });
  });

  describe('isValid', () => {
    it('should return true for valid 24-character hex strings', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd799439011')).toBe(true);
      expect(ObjectId.isValid('ABCDEF1234567890abcdef12')).toBe(true);
    });

    it('should return false for invalid strings', () => {
      expect(ObjectId.isValid('')).toBe(false);
      expect(ObjectId.isValid('123')).toBe(false);
      expect(ObjectId.isValid('not-a-valid-objectid!')).toBe(false);
      expect(ObjectId.isValid('507f1f77bcf86cd79943901')).toBe(false); // 23 chars
      expect(ObjectId.isValid('507f1f77bcf86cd7994390111')).toBe(false); // 25 chars
      expect(ObjectId.isValid('507f1f77bcf86cd79943901g')).toBe(false); // invalid char
    });
  });
});
