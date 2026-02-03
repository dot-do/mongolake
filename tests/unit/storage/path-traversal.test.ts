/**
 * Storage Key Validation Tests - Path Traversal Prevention
 *
 * Tests for the validateStorageKey and validateStoragePrefix functions
 * that prevent path traversal attacks in the FileSystemStorage backend.
 *
 * Security issue: mongolake-a9v
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { mkdir, rm, writeFile, readFile, access } from 'node:fs/promises';
import {
  validateStorageKey,
  validateStoragePrefix,
  InvalidStorageKeyError,
  FileSystemStorage,
  MemoryStorage,
} from '../../../src/storage/index.js';
import { S3Storage } from '../../../src/storage/s3.js';

// ============================================================================
// validateStorageKey Unit Tests
// ============================================================================

describe('validateStorageKey', () => {
  describe('valid keys', () => {
    it('should accept simple keys', () => {
      expect(validateStorageKey('file.txt')).toBe('file.txt');
      expect(validateStorageKey('data')).toBe('data');
    });

    it('should accept keys with subdirectories', () => {
      expect(validateStorageKey('data/file.txt')).toBe('data/file.txt');
      expect(validateStorageKey('a/b/c/d.txt')).toBe('a/b/c/d.txt');
    });

    it('should accept keys with dots in names', () => {
      expect(validateStorageKey('file.name.txt')).toBe('file.name.txt');
      expect(validateStorageKey('.hidden')).toBe('.hidden');
      expect(validateStorageKey('.hidden/file')).toBe('.hidden/file');
    });

    it('should accept keys with special characters', () => {
      expect(validateStorageKey('file-name_v1.txt')).toBe('file-name_v1.txt');
      expect(validateStorageKey('data/2024-01-01/log.txt')).toBe('data/2024-01-01/log.txt');
    });

    it('should accept keys with single dots', () => {
      expect(validateStorageKey('./file.txt')).toBe('./file.txt');
      expect(validateStorageKey('data/./file.txt')).toBe('data/./file.txt');
    });
  });

  describe('path traversal attacks', () => {
    it('should reject ".." as key', () => {
      expect(() => validateStorageKey('..')).toThrow(InvalidStorageKeyError);
      expect(() => validateStorageKey('..')).toThrow('path traversal');
    });

    it('should reject keys starting with "../"', () => {
      expect(() => validateStorageKey('../etc/passwd')).toThrow(InvalidStorageKeyError);
      expect(() => validateStorageKey('../../../etc/passwd')).toThrow(InvalidStorageKeyError);
      expect(() => validateStorageKey('../file.txt')).toThrow(InvalidStorageKeyError);
    });

    it('should reject keys with ".." in the middle', () => {
      expect(() => validateStorageKey('data/../etc/passwd')).toThrow(InvalidStorageKeyError);
      expect(() => validateStorageKey('a/b/../../../etc/passwd')).toThrow(InvalidStorageKeyError);
      expect(() => validateStorageKey('valid/../../../escape')).toThrow(InvalidStorageKeyError);
    });

    it('should reject keys ending with ".."', () => {
      expect(() => validateStorageKey('data/..')).toThrow(InvalidStorageKeyError);
      expect(() => validateStorageKey('a/b/c/..')).toThrow(InvalidStorageKeyError);
    });

    it('should reject Windows-style path traversal', () => {
      expect(() => validateStorageKey('..\\etc\\passwd')).toThrow(InvalidStorageKeyError);
      expect(() => validateStorageKey('data\\..\\..\\etc')).toThrow(InvalidStorageKeyError);
    });
  });

  describe('absolute path attacks', () => {
    it('should reject Unix absolute paths', () => {
      expect(() => validateStorageKey('/etc/passwd')).toThrow(InvalidStorageKeyError);
      expect(() => validateStorageKey('/etc/passwd')).toThrow('absolute path');
      expect(() => validateStorageKey('/var/log/syslog')).toThrow(InvalidStorageKeyError);
      expect(() => validateStorageKey('/')).toThrow(InvalidStorageKeyError);
    });
  });

  describe('empty key validation', () => {
    it('should reject empty keys', () => {
      expect(() => validateStorageKey('')).toThrow(InvalidStorageKeyError);
      expect(() => validateStorageKey('')).toThrow('cannot be empty');
    });

    it('should reject whitespace-only keys', () => {
      expect(() => validateStorageKey('   ')).toThrow(InvalidStorageKeyError);
      expect(() => validateStorageKey('\t\n')).toThrow(InvalidStorageKeyError);
    });
  });

  describe('error messages', () => {
    it('should include the key in error messages for path traversal', () => {
      try {
        validateStorageKey('../secret');
      } catch (e) {
        expect((e as Error).message).toContain('../secret');
      }
    });

    it('should include the key in error messages for absolute paths', () => {
      try {
        validateStorageKey('/etc/passwd');
      } catch (e) {
        expect((e as Error).message).toContain('/etc/passwd');
      }
    });

    it('should have correct error name', () => {
      try {
        validateStorageKey('../secret');
      } catch (e) {
        expect((e as InvalidStorageKeyError).name).toBe('InvalidStorageKeyError');
      }
    });
  });
});

// ============================================================================
// validateStoragePrefix Unit Tests
// ============================================================================

describe('validateStoragePrefix', () => {
  it('should accept empty prefix', () => {
    expect(validateStoragePrefix('')).toBe('');
  });

  it('should accept valid prefixes', () => {
    expect(validateStoragePrefix('data/')).toBe('data/');
    expect(validateStoragePrefix('logs/2024/')).toBe('logs/2024/');
  });

  it('should reject path traversal in prefixes', () => {
    expect(() => validateStoragePrefix('../')).toThrow(InvalidStorageKeyError);
    expect(() => validateStoragePrefix('data/../')).toThrow(InvalidStorageKeyError);
    expect(() => validateStoragePrefix('..')).toThrow(InvalidStorageKeyError);
  });

  it('should reject absolute paths in prefixes', () => {
    expect(() => validateStoragePrefix('/etc/')).toThrow(InvalidStorageKeyError);
  });
});

// ============================================================================
// FileSystemStorage Integration Tests
// ============================================================================

describe('FileSystemStorage - Path Traversal Prevention', () => {
  let storage: FileSystemStorage;
  let testDir: string;
  let sensitiveDir: string;
  let sensitiveFile: string;

  beforeEach(async () => {
    // Create test directories
    testDir = join(tmpdir(), `mongolake-test-${Date.now()}`);
    sensitiveDir = join(tmpdir(), `mongolake-sensitive-${Date.now()}`);

    await mkdir(testDir, { recursive: true });
    await mkdir(sensitiveDir, { recursive: true });

    // Create a "sensitive" file outside the storage base path
    sensitiveFile = join(sensitiveDir, 'secret.txt');
    await writeFile(sensitiveFile, 'SENSITIVE_DATA_SHOULD_NOT_BE_ACCESSIBLE');

    storage = new FileSystemStorage(testDir);
  });

  afterEach(async () => {
    // Clean up test directories
    await rm(testDir, { recursive: true, force: true });
    await rm(sensitiveDir, { recursive: true, force: true });
  });

  describe('get()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.get('../../../etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.get('/etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should allow valid keys', async () => {
      // Write a file first
      await storage.put('valid/file.txt', new Uint8Array([1, 2, 3]));

      // Should be able to read it back
      const data = await storage.get('valid/file.txt');
      expect(data).not.toBeNull();
      expect(Array.from(data!)).toEqual([1, 2, 3]);
    });
  });

  describe('put()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(
        storage.put('../../../tmp/malicious.txt', new Uint8Array([1, 2, 3]))
      ).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path attempts', async () => {
      await expect(
        storage.put('/tmp/malicious.txt', new Uint8Array([1, 2, 3]))
      ).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should allow valid keys', async () => {
      await expect(
        storage.put('valid/nested/file.txt', new Uint8Array([1, 2, 3]))
      ).resolves.not.toThrow();
    });
  });

  describe('delete()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.delete('../../../etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.delete('/etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });
  });

  describe('list()', () => {
    it('should reject path traversal in prefix', async () => {
      await expect(storage.list('../../../')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path prefix', async () => {
      await expect(storage.list('/etc/')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should allow empty prefix', async () => {
      await expect(storage.list('')).resolves.toBeInstanceOf(Array);
    });
  });

  describe('exists()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.exists('../../../etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.exists('/etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });
  });

  describe('head()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.head('../../../etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.head('/etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });
  });

  describe('createMultipartUpload()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.createMultipartUpload('../../../tmp/upload')).rejects.toThrow(
        InvalidStorageKeyError
      );
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.createMultipartUpload('/tmp/upload')).rejects.toThrow(
        InvalidStorageKeyError
      );
    });
  });

  describe('getStream()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.getStream('../../../etc/passwd')).rejects.toThrow(
        InvalidStorageKeyError
      );
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.getStream('/etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });
  });

  describe('putStream()', () => {
    it('should reject path traversal attempts', async () => {
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new Uint8Array([1, 2, 3]));
          controller.close();
        },
      });
      await expect(storage.putStream('../../../tmp/malicious.txt', stream)).rejects.toThrow(
        InvalidStorageKeyError
      );
    });

    it('should reject absolute path attempts', async () => {
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new Uint8Array([1, 2, 3]));
          controller.close();
        },
      });
      await expect(storage.putStream('/tmp/malicious.txt', stream)).rejects.toThrow(
        InvalidStorageKeyError
      );
    });
  });

  describe('attack vector verification', () => {
    it('should not be able to read files outside base directory', async () => {
      // Attempt various escape sequences
      const escapeAttempts = [
        '../secret.txt',
        '../../secret.txt',
        `../${sensitiveDir.split('/').pop()}/secret.txt`,
        'valid/../../secret.txt',
        'valid/../../../secret.txt',
      ];

      for (const attempt of escapeAttempts) {
        await expect(storage.get(attempt)).rejects.toThrow(InvalidStorageKeyError);
      }
    });

    it('should not be able to write files outside base directory', async () => {
      const escapeAttempts = ['../malicious.txt', '../../malicious.txt', 'valid/../../../malicious.txt'];

      for (const attempt of escapeAttempts) {
        await expect(storage.put(attempt, new Uint8Array([1, 2, 3]))).rejects.toThrow(
          InvalidStorageKeyError
        );
      }
    });

    it('should not be able to delete files outside base directory', async () => {
      // Verify our sensitive file exists
      await expect(access(sensitiveFile)).resolves.not.toThrow();

      // Try to delete it via path traversal
      const relPath = `../${sensitiveDir.split('/').pop()}/secret.txt`;
      await expect(storage.delete(relPath)).rejects.toThrow(InvalidStorageKeyError);

      // Verify the file still exists
      await expect(access(sensitiveFile)).resolves.not.toThrow();
      const content = await readFile(sensitiveFile, 'utf8');
      expect(content).toBe('SENSITIVE_DATA_SHOULD_NOT_BE_ACCESSIBLE');
    });
  });
});

// ============================================================================
// Edge Cases and Regression Tests
// ============================================================================

describe('Path Traversal - Edge Cases', () => {
  it('should handle double slashes', () => {
    // Double slashes are fine - they do not enable traversal
    expect(validateStorageKey('data//file.txt')).toBe('data//file.txt');
  });

  it('should handle encoded traversal sequences', () => {
    // URL-encoded .. should still be caught as the raw string
    // Note: actual URL decoding would happen at a different layer
    // Here we just validate the raw key string
    expect(validateStorageKey('data/%2e%2e/secret')).toBe('data/%2e%2e/secret');

    // But literal .. must be rejected
    expect(() => validateStorageKey('data/../secret')).toThrow(InvalidStorageKeyError);
  });

  it('should handle null bytes in key', () => {
    // Keys with null bytes should be handled gracefully
    // The key itself is technically valid but may cause issues downstream
    expect(validateStorageKey('file\x00.txt')).toBe('file\x00.txt');
  });

  it('should handle unicode path separators', () => {
    // Unicode characters that might be confused with path separators
    expect(validateStorageKey('data\u2044file.txt')).toBe('data\u2044file.txt'); // Fraction slash
    expect(validateStorageKey('data\uff0ffile.txt')).toBe('data\uff0ffile.txt'); // Fullwidth solidus
  });

  it('should handle very long paths', () => {
    const longPath = 'a/'.repeat(100) + 'file.txt';
    expect(validateStorageKey(longPath)).toBe(longPath);
  });

  it('should handle mixed traversal attempts', () => {
    expect(() => validateStorageKey('valid/path/../../../etc/passwd')).toThrow(
      InvalidStorageKeyError
    );
  });
});

// ============================================================================
// MemoryStorage - Path Traversal Prevention
// ============================================================================

describe('MemoryStorage - Path Traversal Prevention', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
  });

  afterEach(() => {
    storage.clear();
  });

  describe('get()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.get('../../../etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.get('/etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should allow valid keys', async () => {
      await storage.put('valid/file.txt', new Uint8Array([1, 2, 3]));
      const data = await storage.get('valid/file.txt');
      expect(data).not.toBeNull();
      expect(Array.from(data!)).toEqual([1, 2, 3]);
    });
  });

  describe('put()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(
        storage.put('../../../tmp/malicious.txt', new Uint8Array([1, 2, 3]))
      ).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path attempts', async () => {
      await expect(
        storage.put('/tmp/malicious.txt', new Uint8Array([1, 2, 3]))
      ).rejects.toThrow(InvalidStorageKeyError);
    });
  });

  describe('delete()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.delete('../../../etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.delete('/etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });
  });

  describe('list()', () => {
    it('should reject path traversal in prefix', async () => {
      await expect(storage.list('../../../')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path prefix', async () => {
      await expect(storage.list('/etc/')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should allow empty prefix', async () => {
      await expect(storage.list('')).resolves.toBeInstanceOf(Array);
    });
  });

  describe('exists()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.exists('../../../etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.exists('/etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });
  });

  describe('head()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.head('../../../etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.head('/etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });
  });

  describe('createMultipartUpload()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.createMultipartUpload('../../../tmp/upload')).rejects.toThrow(
        InvalidStorageKeyError
      );
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.createMultipartUpload('/tmp/upload')).rejects.toThrow(
        InvalidStorageKeyError
      );
    });
  });

  describe('getStream()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.getStream('../../../etc/passwd')).rejects.toThrow(
        InvalidStorageKeyError
      );
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.getStream('/etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });
  });

  describe('putStream()', () => {
    it('should reject path traversal attempts', async () => {
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new Uint8Array([1, 2, 3]));
          controller.close();
        },
      });
      await expect(storage.putStream('../../../tmp/malicious.txt', stream)).rejects.toThrow(
        InvalidStorageKeyError
      );
    });

    it('should reject absolute path attempts', async () => {
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new Uint8Array([1, 2, 3]));
          controller.close();
        },
      });
      await expect(storage.putStream('/tmp/malicious.txt', stream)).rejects.toThrow(
        InvalidStorageKeyError
      );
    });
  });
});

// ============================================================================
// S3Storage - Path Traversal Prevention
// ============================================================================

describe('S3Storage - Path Traversal Prevention', () => {
  let storage: S3Storage;

  beforeEach(() => {
    // Create S3Storage with mock config (we only test validation, not actual S3 calls)
    storage = new S3Storage({
      endpoint: 'https://s3.example.com',
      accessKeyId: 'test-key',
      secretAccessKey: 'test-secret',
      bucket: 'test-bucket',
    });
  });

  describe('get()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.get('../../../etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.get('/etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });
  });

  describe('put()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(
        storage.put('../../../tmp/malicious.txt', new Uint8Array([1, 2, 3]))
      ).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path attempts', async () => {
      await expect(
        storage.put('/tmp/malicious.txt', new Uint8Array([1, 2, 3]))
      ).rejects.toThrow(InvalidStorageKeyError);
    });
  });

  describe('delete()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.delete('../../../etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.delete('/etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });
  });

  describe('list()', () => {
    it('should reject path traversal in prefix', async () => {
      await expect(storage.list('../../../')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path prefix', async () => {
      await expect(storage.list('/etc/')).rejects.toThrow(InvalidStorageKeyError);
    });
  });

  describe('exists()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.exists('../../../etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.exists('/etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });
  });

  describe('head()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.head('../../../etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.head('/etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });
  });

  describe('createMultipartUpload()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.createMultipartUpload('../../../tmp/upload')).rejects.toThrow(
        InvalidStorageKeyError
      );
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.createMultipartUpload('/tmp/upload')).rejects.toThrow(
        InvalidStorageKeyError
      );
    });
  });

  describe('getStream()', () => {
    it('should reject path traversal attempts', async () => {
      await expect(storage.getStream('../../../etc/passwd')).rejects.toThrow(
        InvalidStorageKeyError
      );
    });

    it('should reject absolute path attempts', async () => {
      await expect(storage.getStream('/etc/passwd')).rejects.toThrow(InvalidStorageKeyError);
    });
  });

  describe('putStream()', () => {
    it('should reject path traversal attempts', async () => {
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new Uint8Array([1, 2, 3]));
          controller.close();
        },
      });
      await expect(storage.putStream('../../../tmp/malicious.txt', stream)).rejects.toThrow(
        InvalidStorageKeyError
      );
    });

    it('should reject absolute path attempts', async () => {
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new Uint8Array([1, 2, 3]));
          controller.close();
        },
      });
      await expect(storage.putStream('/tmp/malicious.txt', stream)).rejects.toThrow(
        InvalidStorageKeyError
      );
    });
  });
});
