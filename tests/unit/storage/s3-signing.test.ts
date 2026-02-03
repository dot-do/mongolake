/**
 * S3 AWS Signature Version 4 Signing Tests
 *
 * Tests for the AWS SigV4 signing implementation:
 * - toHex conversion
 * - sha256Hex hashing
 * - hmacSha256 computation
 * - getSigningKey derivation
 * - formatAmzDate and formatDateStamp
 * - awsUriEncode
 * - normalizeHeaderValue
 *
 * Uses AWS test vectors where available to verify correctness.
 */

import { describe, it, expect } from 'vitest';
import {
  toHex,
  sha256Hex,
  hmacSha256,
  getSigningKey,
  formatAmzDate,
  formatDateStamp,
  awsUriEncode,
  normalizeHeaderValue,
} from '../../../src/storage/s3.js';

// ============================================================================
// toHex Tests
// ============================================================================

describe('toHex', () => {
  it('should convert empty array to empty string', () => {
    expect(toHex(new Uint8Array([]))).toBe('');
  });

  it('should convert single byte correctly', () => {
    expect(toHex(new Uint8Array([0]))).toBe('00');
    expect(toHex(new Uint8Array([15]))).toBe('0f');
    expect(toHex(new Uint8Array([16]))).toBe('10');
    expect(toHex(new Uint8Array([255]))).toBe('ff');
  });

  it('should convert multiple bytes correctly', () => {
    expect(toHex(new Uint8Array([1, 2, 3]))).toBe('010203');
    expect(toHex(new Uint8Array([0xde, 0xad, 0xbe, 0xef]))).toBe('deadbeef');
  });

  it('should produce lowercase hex', () => {
    const result = toHex(new Uint8Array([0xab, 0xcd, 0xef]));
    expect(result).toBe('abcdef');
    expect(result).toBe(result.toLowerCase());
  });
});

// ============================================================================
// sha256Hex Tests
// ============================================================================

describe('sha256Hex', () => {
  it('should hash empty string correctly', async () => {
    // SHA-256 of empty string is well-known
    const result = await sha256Hex('');
    expect(result).toBe('e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855');
  });

  it('should hash empty Uint8Array correctly', async () => {
    const result = await sha256Hex(new Uint8Array([]));
    expect(result).toBe('e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855');
  });

  it('should hash "hello" correctly', async () => {
    // SHA-256 of "hello" is well-known
    const result = await sha256Hex('hello');
    expect(result).toBe('2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824');
  });

  it('should hash Uint8Array of "hello" correctly', async () => {
    const encoder = new TextEncoder();
    const result = await sha256Hex(encoder.encode('hello'));
    expect(result).toBe('2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824');
  });

  it('should produce lowercase hex output', async () => {
    const result = await sha256Hex('test');
    expect(result).toBe(result.toLowerCase());
  });

  it('should produce 64 character output', async () => {
    const result = await sha256Hex('any input');
    expect(result.length).toBe(64);
  });
});

// ============================================================================
// hmacSha256 Tests
// ============================================================================

describe('hmacSha256', () => {
  it('should compute HMAC-SHA256 correctly', async () => {
    // Known test vector
    const key = new TextEncoder().encode('key');
    const result = await hmacSha256(key, 'The quick brown fox jumps over the lazy dog');
    const hex = toHex(result);
    expect(hex).toBe('f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8');
  });

  it('should produce 32 byte output', async () => {
    const key = new TextEncoder().encode('any key');
    const result = await hmacSha256(key, 'any message');
    expect(result.length).toBe(32);
  });

  it('should produce different outputs for different keys', async () => {
    const key1 = new TextEncoder().encode('key1');
    const key2 = new TextEncoder().encode('key2');
    const message = 'same message';

    const result1 = await hmacSha256(key1, message);
    const result2 = await hmacSha256(key2, message);

    expect(toHex(result1)).not.toBe(toHex(result2));
  });

  it('should produce different outputs for different messages', async () => {
    const key = new TextEncoder().encode('same key');
    const result1 = await hmacSha256(key, 'message1');
    const result2 = await hmacSha256(key, 'message2');

    expect(toHex(result1)).not.toBe(toHex(result2));
  });
});

// ============================================================================
// getSigningKey Tests
// ============================================================================

describe('getSigningKey', () => {
  it('should derive signing key using AWS test vector', async () => {
    // AWS provides test vectors in their documentation
    // Secret key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
    // Date: 20150830
    // Region: us-east-1
    // Service: iam
    // Expected signing key (hex): c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9
    const signingKey = await getSigningKey(
      'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY',
      '20150830',
      'us-east-1',
      'iam'
    );
    expect(toHex(signingKey)).toBe('c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9');
  });

  it('should produce 32 byte signing key', async () => {
    const signingKey = await getSigningKey('testSecret', '20230101', 'us-west-2', 's3');
    expect(signingKey.length).toBe(32);
  });

  it('should produce different keys for different regions', async () => {
    const key1 = await getSigningKey('testSecret', '20230101', 'us-east-1', 's3');
    const key2 = await getSigningKey('testSecret', '20230101', 'us-west-2', 's3');

    expect(toHex(key1)).not.toBe(toHex(key2));
  });

  it('should produce different keys for different services', async () => {
    const key1 = await getSigningKey('testSecret', '20230101', 'us-east-1', 's3');
    const key2 = await getSigningKey('testSecret', '20230101', 'us-east-1', 'iam');

    expect(toHex(key1)).not.toBe(toHex(key2));
  });

  it('should produce different keys for different dates', async () => {
    const key1 = await getSigningKey('testSecret', '20230101', 'us-east-1', 's3');
    const key2 = await getSigningKey('testSecret', '20230102', 'us-east-1', 's3');

    expect(toHex(key1)).not.toBe(toHex(key2));
  });
});

// ============================================================================
// formatAmzDate Tests
// ============================================================================

describe('formatAmzDate', () => {
  it('should format date in AWS format', () => {
    const date = new Date(Date.UTC(2023, 5, 15, 10, 30, 45)); // June 15, 2023 10:30:45 UTC
    expect(formatAmzDate(date)).toBe('20230615T103045Z');
  });

  it('should pad single digit components', () => {
    const date = new Date(Date.UTC(2023, 0, 5, 8, 5, 3)); // Jan 5, 2023 08:05:03 UTC
    expect(formatAmzDate(date)).toBe('20230105T080503Z');
  });

  it('should handle midnight correctly', () => {
    const date = new Date(Date.UTC(2023, 11, 31, 0, 0, 0)); // Dec 31, 2023 00:00:00 UTC
    expect(formatAmzDate(date)).toBe('20231231T000000Z');
  });

  it('should handle end of day correctly', () => {
    const date = new Date(Date.UTC(2023, 0, 1, 23, 59, 59)); // Jan 1, 2023 23:59:59 UTC
    expect(formatAmzDate(date)).toBe('20230101T235959Z');
  });

  it('should use UTC time', () => {
    // Create a date and format it - result should always use UTC
    const date = new Date(Date.UTC(2023, 6, 4, 12, 0, 0));
    const result = formatAmzDate(date);
    expect(result).toBe('20230704T120000Z');
    expect(result).toMatch(/^\d{8}T\d{6}Z$/);
  });
});

// ============================================================================
// formatDateStamp Tests
// ============================================================================

describe('formatDateStamp', () => {
  it('should format date stamp in AWS format', () => {
    const date = new Date(Date.UTC(2023, 5, 15, 10, 30, 45));
    expect(formatDateStamp(date)).toBe('20230615');
  });

  it('should pad single digit month and day', () => {
    const date = new Date(Date.UTC(2023, 0, 5, 8, 5, 3));
    expect(formatDateStamp(date)).toBe('20230105');
  });

  it('should handle December correctly', () => {
    const date = new Date(Date.UTC(2023, 11, 31, 23, 59, 59));
    expect(formatDateStamp(date)).toBe('20231231');
  });

  it('should produce 8 character output', () => {
    const date = new Date();
    expect(formatDateStamp(date).length).toBe(8);
  });
});

// ============================================================================
// awsUriEncode Tests
// ============================================================================

describe('awsUriEncode', () => {
  it('should not encode unreserved characters', () => {
    const unreserved = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~';
    expect(awsUriEncode(unreserved)).toBe(unreserved);
  });

  it('should encode spaces as %20', () => {
    expect(awsUriEncode('hello world')).toBe('hello%20world');
  });

  it('should encode special characters', () => {
    expect(awsUriEncode('a+b')).toBe('a%2Bb');
    expect(awsUriEncode('a&b')).toBe('a%26b');
    expect(awsUriEncode('a=b')).toBe('a%3Db');
    expect(awsUriEncode('a?b')).toBe('a%3Fb');
  });

  it('should encode slashes by default', () => {
    expect(awsUriEncode('a/b', true)).toBe('a%2Fb');
    expect(awsUriEncode('a/b')).toBe('a%2Fb');
  });

  it('should not encode slashes when encodeSlash is false', () => {
    expect(awsUriEncode('a/b/c', false)).toBe('a/b/c');
  });

  it('should use uppercase hex encoding', () => {
    expect(awsUriEncode(' ')).toBe('%20');
    expect(awsUriEncode('\n')).toBe('%0A');
    expect(awsUriEncode('\xff')).not.toContain('ff'); // Should be uppercase
  });

  it('should handle UTF-8 multi-byte characters', () => {
    // UTF-8 encoding of euro sign is E2 82 AC
    expect(awsUriEncode('\u20AC')).toBe('%E2%82%AC');
  });

  it('should handle empty string', () => {
    expect(awsUriEncode('')).toBe('');
  });

  it('should handle path-like strings', () => {
    expect(awsUriEncode('folder/subfolder/file.txt', false)).toBe('folder/subfolder/file.txt');
    expect(awsUriEncode('folder with spaces/file.txt', false)).toBe('folder%20with%20spaces/file.txt');
  });
});

// ============================================================================
// normalizeHeaderValue Tests
// ============================================================================

describe('normalizeHeaderValue', () => {
  it('should trim leading whitespace', () => {
    expect(normalizeHeaderValue('  value')).toBe('value');
    expect(normalizeHeaderValue('\t\nvalue')).toBe('value');
  });

  it('should trim trailing whitespace', () => {
    expect(normalizeHeaderValue('value  ')).toBe('value');
    expect(normalizeHeaderValue('value\t\n')).toBe('value');
  });

  it('should collapse multiple spaces to single space', () => {
    expect(normalizeHeaderValue('a  b')).toBe('a b');
    expect(normalizeHeaderValue('a    b')).toBe('a b');
  });

  it('should collapse tabs and newlines to single space', () => {
    expect(normalizeHeaderValue('a\t\nb')).toBe('a b');
    expect(normalizeHeaderValue('a \t \n b')).toBe('a b');
  });

  it('should handle combined cases', () => {
    expect(normalizeHeaderValue('  a  b  c  ')).toBe('a b c');
  });

  it('should return empty string for whitespace-only input', () => {
    expect(normalizeHeaderValue('   ')).toBe('');
    expect(normalizeHeaderValue('\t\n')).toBe('');
  });

  it('should preserve single value unchanged', () => {
    expect(normalizeHeaderValue('value')).toBe('value');
  });
});

// ============================================================================
// Integration: Complete Signing Flow
// ============================================================================

describe('S3 Signing Integration', () => {
  it('should derive consistent signing key for same inputs', async () => {
    const key1 = await getSigningKey('secret', '20230101', 'us-east-1', 's3');
    const key2 = await getSigningKey('secret', '20230101', 'us-east-1', 's3');
    expect(toHex(key1)).toBe(toHex(key2));
  });

  it('should produce deterministic signature components', async () => {
    const date = new Date(Date.UTC(2023, 0, 15, 12, 0, 0));
    const amzDate = formatAmzDate(date);
    const dateStamp = formatDateStamp(date);

    expect(amzDate).toBe('20230115T120000Z');
    expect(dateStamp).toBe('20230115');

    // Signing key should be deterministic
    const signingKey = await getSigningKey('testSecret', dateStamp, 'us-east-1', 's3');
    const signingKey2 = await getSigningKey('testSecret', dateStamp, 'us-east-1', 's3');
    expect(toHex(signingKey)).toBe(toHex(signingKey2));
  });

  it('should handle full path encoding correctly', () => {
    const path = 'data/2023/test file.parquet';
    const encoded = path
      .split('/')
      .map((segment) => awsUriEncode(segment, true))
      .join('/');
    expect(encoded).toBe('data/2023/test%20file.parquet');
  });
});
