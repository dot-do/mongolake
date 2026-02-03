/**
 * Range Handler Tests
 *
 * Tests for HTTP Range request handling, including:
 * - Range header parsing
 * - Byte ranges (bytes=0-1023)
 * - Suffix ranges (bytes=-1024)
 * - 206 Partial Content responses
 * - Content-Range headers
 * - Integration with storage backends
 * - Parquet footer reading
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  parseRangeHeader,
  isRangeSatisfiable,
  RangeHandler,
  StorageRangeHandler,
  ParquetFooterReader,
  type ByteRange,
  type ParsedRange,
  type RangeResponse,
} from '../../../src/storage/range-handler.js';
import { MemoryStorage } from '../../../src/storage/index.js';

// ============================================================================
// Test Data
// ============================================================================

/** Create test data of specified size */
function createTestData(size: number): Uint8Array {
  const data = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    data[i] = i % 256;
  }
  return data;
}

/** Create a minimal valid Parquet file */
function createMinimalParquetFile(): Uint8Array {
  // Minimal Parquet file structure:
  // [magic "PAR1" (4)] [some data] [footer metadata] [footer length (4 LE)] [magic "PAR1" (4)]
  const magic = new Uint8Array([0x50, 0x41, 0x52, 0x31]); // "PAR1"

  // Simple footer metadata (just a placeholder)
  const footerMetadata = new Uint8Array([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
  const footerLength = footerMetadata.byteLength;

  // Footer length as 4-byte little-endian
  const footerLengthBytes = new Uint8Array(4);
  new DataView(footerLengthBytes.buffer).setUint32(0, footerLength, true);

  // Combine: magic + padding + footerMetadata + footerLengthBytes + magic
  const totalSize = 4 + 100 + footerLength + 4 + 4; // magic + data + footer + len + magic
  const file = new Uint8Array(totalSize);

  let offset = 0;

  // Start magic
  file.set(magic, offset);
  offset += 4;

  // Some row group data (padding)
  for (let i = 0; i < 100; i++) {
    file[offset + i] = i % 256;
  }
  offset += 100;

  // Footer metadata
  file.set(footerMetadata, offset);
  offset += footerLength;

  // Footer length
  file.set(footerLengthBytes, offset);
  offset += 4;

  // End magic
  file.set(magic, offset);

  return file;
}

// ============================================================================
// parseRangeHeader Tests
// ============================================================================

describe('parseRangeHeader', () => {
  describe('basic byte ranges', () => {
    it('should parse simple byte range', () => {
      const result = parseRangeHeader('bytes=0-1023', 10000);

      expect(result).not.toBeNull();
      expect(result!.unit).toBe('bytes');
      expect(result!.ranges).toHaveLength(1);
      expect(result!.ranges[0]).toEqual({ start: 0, end: 1023 });
    });

    it('should parse range starting from middle', () => {
      const result = parseRangeHeader('bytes=500-999', 10000);

      expect(result).not.toBeNull();
      expect(result!.ranges[0]).toEqual({ start: 500, end: 999 });
    });

    it('should parse range at end of file', () => {
      const result = parseRangeHeader('bytes=9000-9999', 10000);

      expect(result).not.toBeNull();
      expect(result!.ranges[0]).toEqual({ start: 9000, end: 9999 });
    });

    it('should cap end at file size - 1', () => {
      const result = parseRangeHeader('bytes=0-50000', 10000);

      expect(result).not.toBeNull();
      expect(result!.ranges[0]).toEqual({ start: 0, end: 9999 });
    });
  });

  describe('suffix ranges', () => {
    it('should parse suffix range (last N bytes)', () => {
      const result = parseRangeHeader('bytes=-1024', 10000);

      expect(result).not.toBeNull();
      expect(result!.ranges[0]).toEqual({ start: 8976, end: 9999 });
    });

    it('should parse small suffix range', () => {
      const result = parseRangeHeader('bytes=-8', 100);

      expect(result).not.toBeNull();
      expect(result!.ranges[0]).toEqual({ start: 92, end: 99 });
    });

    it('should handle suffix larger than file size', () => {
      const result = parseRangeHeader('bytes=-50000', 10000);

      expect(result).not.toBeNull();
      expect(result!.ranges[0]).toEqual({ start: 0, end: 9999 });
    });
  });

  describe('open-ended ranges', () => {
    it('should parse open-ended range (from N to end)', () => {
      const result = parseRangeHeader('bytes=5000-', 10000);

      expect(result).not.toBeNull();
      expect(result!.ranges[0]).toEqual({ start: 5000, end: 9999 });
    });

    it('should parse from start to end', () => {
      const result = parseRangeHeader('bytes=0-', 10000);

      expect(result).not.toBeNull();
      expect(result!.ranges[0]).toEqual({ start: 0, end: 9999 });
    });
  });

  describe('multiple ranges', () => {
    it('should parse multiple ranges', () => {
      const result = parseRangeHeader('bytes=0-100,200-300,500-600', 10000);

      expect(result).not.toBeNull();
      expect(result!.ranges).toHaveLength(3);
      expect(result!.ranges[0]).toEqual({ start: 0, end: 100 });
      expect(result!.ranges[1]).toEqual({ start: 200, end: 300 });
      expect(result!.ranges[2]).toEqual({ start: 500, end: 600 });
    });

    it('should handle spaces in multiple ranges', () => {
      const result = parseRangeHeader('bytes=0-100, 200-300', 10000);

      expect(result).not.toBeNull();
      expect(result!.ranges).toHaveLength(2);
    });
  });

  describe('invalid ranges', () => {
    it('should return null for missing equals sign', () => {
      const result = parseRangeHeader('bytes0-1023', 10000);
      expect(result).toBeNull();
    });

    it('should return null for non-bytes unit', () => {
      const result = parseRangeHeader('items=0-10', 10000);
      expect(result).toBeNull();
    });

    it('should return null for end before start', () => {
      const result = parseRangeHeader('bytes=1000-500', 10000);
      expect(result).toBeNull();
    });

    it('should return null for negative start', () => {
      const result = parseRangeHeader('bytes=-10-100', 10000);
      expect(result).toBeNull();
    });

    it('should parse range even when start is beyond file size', () => {
      const result = parseRangeHeader('bytes=50000-50100', 10000);
      // parseRangeHeader parses syntactically valid ranges; satisfiability is checked separately
      expect(result).not.toBeNull();
      expect(result!.unit).toBe('bytes');
      // The caller should use isRangeSatisfiable() to check if this range is valid
    });

    it('should return null for invalid suffix (zero)', () => {
      const result = parseRangeHeader('bytes=-0', 10000);
      expect(result).toBeNull();
    });

    it('should return null for empty range', () => {
      const result = parseRangeHeader('bytes=', 10000);
      expect(result).toBeNull();
    });

    it('should return null for garbage input', () => {
      const result = parseRangeHeader('not a range header', 10000);
      expect(result).toBeNull();
    });
  });

  describe('edge cases', () => {
    it('should handle single byte range', () => {
      const result = parseRangeHeader('bytes=0-0', 10000);

      expect(result).not.toBeNull();
      expect(result!.ranges[0]).toEqual({ start: 0, end: 0 });
    });

    it('should parse range for empty file', () => {
      const result = parseRangeHeader('bytes=0-100', 0);
      // parseRangeHeader parses syntactically valid ranges even for empty files;
      // satisfiability is checked separately via isRangeSatisfiable()
      expect(result).not.toBeNull();
      expect(result!.unit).toBe('bytes');
      // Note: end becomes -1 when fileSize is 0, caller uses isRangeSatisfiable() to handle this
    });

    it('should handle very large file', () => {
      const fileSize = 1024 * 1024 * 1024; // 1GB
      const result = parseRangeHeader('bytes=-1024', fileSize);

      expect(result).not.toBeNull();
      expect(result!.ranges[0].start).toBe(fileSize - 1024);
    });
  });
});

// ============================================================================
// isRangeSatisfiable Tests
// ============================================================================

describe('isRangeSatisfiable', () => {
  it('should return true for valid range', () => {
    const ranges: ByteRange[] = [{ start: 0, end: 100 }];
    expect(isRangeSatisfiable(ranges, 1000)).toBe(true);
  });

  it('should return true if any range is satisfiable', () => {
    const ranges: ByteRange[] = [
      { start: 5000, end: 6000 }, // Invalid
      { start: 0, end: 100 }, // Valid
    ];
    expect(isRangeSatisfiable(ranges, 1000)).toBe(true);
  });

  it('should return false for empty file', () => {
    const ranges: ByteRange[] = [{ start: 0, end: 100 }];
    expect(isRangeSatisfiable(ranges, 0)).toBe(false);
  });

  it('should return false when all ranges start beyond file', () => {
    const ranges: ByteRange[] = [
      { start: 2000, end: 3000 },
      { start: 5000, end: 6000 },
    ];
    expect(isRangeSatisfiable(ranges, 1000)).toBe(false);
  });
});

// ============================================================================
// RangeHandler Tests
// ============================================================================

describe('RangeHandler', () => {
  let handler: RangeHandler;
  let testData: Uint8Array;

  beforeEach(() => {
    handler = new RangeHandler();
    testData = createTestData(1000);
  });

  describe('handleRange - no Range header', () => {
    it('should return full file with 200 status', () => {
      const result = handler.handleRange(null, testData);

      expect(result.status).toBe(200);
      expect(result.body).toEqual(testData);
      expect(result.headers['Content-Length']).toBe('1000');
      expect(result.headers['Accept-Ranges']).toBe('bytes');
    });
  });

  describe('handleRange - valid byte range', () => {
    it('should return 206 Partial Content', () => {
      const result = handler.handleRange('bytes=0-99', testData);

      expect(result.status).toBe(206);
    });

    it('should return correct slice of data', () => {
      const result = handler.handleRange('bytes=0-99', testData);

      expect(result.body).toEqual(testData.slice(0, 100));
    });

    it('should set Content-Range header', () => {
      const result = handler.handleRange('bytes=0-99', testData);

      expect(result.headers['Content-Range']).toBe('bytes 0-99/1000');
    });

    it('should set correct Content-Length', () => {
      const result = handler.handleRange('bytes=0-99', testData);

      expect(result.headers['Content-Length']).toBe('100');
    });

    it('should include Accept-Ranges header', () => {
      const result = handler.handleRange('bytes=0-99', testData);

      expect(result.headers['Accept-Ranges']).toBe('bytes');
    });

    it('should return contentRange info', () => {
      const result = handler.handleRange('bytes=0-99', testData);

      expect(result.contentRange).toEqual({
        start: 0,
        end: 99,
        total: 1000,
      });
    });
  });

  describe('handleRange - suffix range', () => {
    it('should return last N bytes', () => {
      const result = handler.handleRange('bytes=-100', testData);

      expect(result.status).toBe(206);
      expect(result.body).toEqual(testData.slice(900, 1000));
    });

    it('should set correct Content-Range', () => {
      const result = handler.handleRange('bytes=-100', testData);

      expect(result.headers['Content-Range']).toBe('bytes 900-999/1000');
    });
  });

  describe('handleRange - open-ended range', () => {
    it('should return from start to end', () => {
      const result = handler.handleRange('bytes=500-', testData);

      expect(result.status).toBe(206);
      expect(result.body).toEqual(testData.slice(500, 1000));
      expect(result.headers['Content-Range']).toBe('bytes 500-999/1000');
    });
  });

  describe('handleRange - unsatisfiable range', () => {
    it('should return 416 for range beyond file', () => {
      const result = handler.handleRange('bytes=2000-3000', testData);

      expect(result.status).toBe(416);
      expect(result.body).toBeNull();
      expect(result.headers['Content-Range']).toBe('bytes */1000');
    });
  });

  describe('handleRange - malformed range', () => {
    it('should return full file for malformed range', () => {
      const result = handler.handleRange('invalid', testData);

      expect(result.status).toBe(200);
      expect(result.body).toEqual(testData);
    });
  });

  describe('toResponse', () => {
    it('should create Response object from 200 result', () => {
      const result = handler.handleRange(null, testData);
      const response = handler.toResponse(result);

      expect(response.status).toBe(200);
    });

    it('should create Response object from 206 result', () => {
      const result = handler.handleRange('bytes=0-99', testData);
      const response = handler.toResponse(result);

      expect(response.status).toBe(206);
    });

    it('should create Response object from 416 result', () => {
      const result = handler.handleRange('bytes=5000-6000', testData);
      const response = handler.toResponse(result);

      expect(response.status).toBe(416);
    });
  });

  describe('multiple ranges', () => {
    it('should use first range only for multiple ranges', () => {
      const result = handler.handleRange('bytes=0-10,20-30,40-50', testData);

      // Should only use first range (multipart/byteranges is rarely used)
      expect(result.status).toBe(206);
      expect(result.body).toEqual(testData.slice(0, 11));
      expect(result.headers['Content-Range']).toBe('bytes 0-10/1000');
    });
  });
});

// ============================================================================
// StorageRangeHandler Tests
// ============================================================================

describe('StorageRangeHandler', () => {
  let storage: MemoryStorage;
  let handler: StorageRangeHandler;
  const testKey = 'test/file.parquet';

  beforeEach(async () => {
    storage = new MemoryStorage();
    handler = new StorageRangeHandler(storage);

    // Store test data
    const testData = createTestData(1000);
    await storage.put(testKey, testData);
  });

  describe('handleRange - full file', () => {
    it('should return full file with 200 status', async () => {
      const result = await handler.handleRange(testKey, null);

      expect(result.status).toBe(200);
      expect(result.body?.byteLength).toBe(1000);
    });
  });

  describe('handleRange - byte range', () => {
    it('should return partial content', async () => {
      const result = await handler.handleRange(testKey, 'bytes=0-99');

      expect(result.status).toBe(206);
      expect(result.body?.byteLength).toBe(100);
      expect(result.headers['Content-Range']).toBe('bytes 0-99/1000');
    });
  });

  describe('handleRange - suffix range', () => {
    it('should return last N bytes', async () => {
      const result = await handler.handleRange(testKey, 'bytes=-100');

      expect(result.status).toBe(206);
      expect(result.body?.byteLength).toBe(100);
    });
  });

  describe('handleRange - missing file', () => {
    it('should return 416 for missing file', async () => {
      const result = await handler.handleRange('nonexistent', null);

      expect(result.status).toBe(416);
      expect(result.body).toBeNull();
    });
  });

  describe('handleRange - invalid range', () => {
    it('should return 416 for invalid range', async () => {
      const result = await handler.handleRange(testKey, 'bytes=5000-6000');

      expect(result.status).toBe(416);
    });
  });
});

// ============================================================================
// ParquetFooterReader Tests
// ============================================================================

describe('ParquetFooterReader', () => {
  let reader: ParquetFooterReader;

  beforeEach(() => {
    reader = new ParquetFooterReader();
  });

  describe('readFooter', () => {
    it('should read footer from valid Parquet file', () => {
      const parquetFile = createMinimalParquetFile();
      const footer = reader.readFooter(parquetFile);

      expect(footer).not.toBeNull();
      expect(footer).toBeInstanceOf(Uint8Array);
    });

    it('should return correct footer content', () => {
      const parquetFile = createMinimalParquetFile();
      const footer = reader.readFooter(parquetFile);

      // Footer metadata is [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
      expect(footer).toEqual(new Uint8Array([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]));
    });

    it('should return null for file too small', () => {
      const tinyFile = new Uint8Array(10);
      const footer = reader.readFooter(tinyFile);

      expect(footer).toBeNull();
    });

    it('should return null for invalid magic bytes', () => {
      const invalidFile = createMinimalParquetFile();
      // Corrupt magic bytes
      invalidFile[invalidFile.length - 1] = 0x00;

      const footer = reader.readFooter(invalidFile);
      expect(footer).toBeNull();
    });

    it('should return null for invalid footer length', () => {
      const corruptFile = createMinimalParquetFile();
      // Set footer length to something larger than file
      const view = new DataView(corruptFile.buffer);
      view.setUint32(corruptFile.length - 8, 99999, true);

      const footer = reader.readFooter(corruptFile);
      expect(footer).toBeNull();
    });
  });

  describe('readFooterWithRanges', () => {
    it('should read footer using range requests', () => {
      const parquetFile = createMinimalParquetFile();
      const footer = reader.readFooterWithRanges(parquetFile);

      expect(footer).not.toBeNull();
      expect(footer).toEqual(new Uint8Array([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]));
    });

    it('should return null for file too small', () => {
      const tinyFile = new Uint8Array(10);
      const footer = reader.readFooterWithRanges(tinyFile);

      expect(footer).toBeNull();
    });

    it('should return null for invalid magic', () => {
      const invalidFile = createMinimalParquetFile();
      invalidFile[invalidFile.length - 1] = 0x00;

      const footer = reader.readFooterWithRanges(invalidFile);
      expect(footer).toBeNull();
    });
  });

  describe('getFooterRangeHeader', () => {
    it('should return correct range header', () => {
      // For a 1000 byte footer, we need footer + 8 bytes (length + magic)
      const header = ParquetFooterReader.getFooterRangeHeader(1000);
      expect(header).toBe('bytes=-1008');
    });

    it('should handle small footer', () => {
      const header = ParquetFooterReader.getFooterRangeHeader(8);
      expect(header).toBe('bytes=-16');
    });
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('Range Handler Integration', () => {
  describe('Parquet footer reading workflow', () => {
    it('should efficiently read Parquet footer from storage', async () => {
      // Setup
      const storage = new MemoryStorage();
      const parquetFile = createMinimalParquetFile();
      await storage.put('data/file.parquet', parquetFile);

      // Create storage range handler
      const rangeHandler = new StorageRangeHandler(storage);

      // Step 1: Read trailer (last 8 bytes) to get footer length
      const trailerResult = await rangeHandler.handleRange(
        'data/file.parquet',
        'bytes=-8'
      );

      expect(trailerResult.status).toBe(206);
      expect(trailerResult.body?.byteLength).toBe(8);

      // Verify magic
      const trailer = trailerResult.body!;
      const magic = trailer.slice(4);
      expect(magic[0]).toBe(0x50); // 'P'
      expect(magic[1]).toBe(0x41); // 'A'
      expect(magic[2]).toBe(0x52); // 'R'
      expect(magic[3]).toBe(0x31); // '1'

      // Get footer length
      const footerLength = new DataView(
        trailer.buffer,
        trailer.byteOffset,
        4
      ).getUint32(0, true);

      expect(footerLength).toBe(8);

      // Step 2: Read full footer
      const footerResult = await rangeHandler.handleRange(
        'data/file.parquet',
        `bytes=-${footerLength + 8}`
      );

      expect(footerResult.status).toBe(206);
      expect(footerResult.body?.byteLength).toBe(footerLength + 8);

      // Extract just the footer metadata
      const footerMetadata = footerResult.body!.slice(0, footerLength);
      expect(footerMetadata).toEqual(
        new Uint8Array([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])
      );
    });
  });

  describe('Response creation', () => {
    it('should create proper HTTP Response for range request', () => {
      const handler = new RangeHandler();
      const data = createTestData(1000);

      const result = handler.handleRange('bytes=0-99', data);
      const response = handler.toResponse(result);

      expect(response.status).toBe(206);
      expect(response.headers.get('Content-Range')).toBe('bytes 0-99/1000');
      expect(response.headers.get('Content-Length')).toBe('100');
      expect(response.headers.get('Accept-Ranges')).toBe('bytes');
    });
  });
});

// ============================================================================
// Edge Case Tests
// ============================================================================

describe('Edge Cases', () => {
  describe('empty file', () => {
    it('should handle empty file gracefully', () => {
      const handler = new RangeHandler();
      const emptyData = new Uint8Array(0);

      const result = handler.handleRange(null, emptyData);
      expect(result.status).toBe(200);
      expect(result.body?.byteLength).toBe(0);
    });

    it('should return 416 for any range on empty file', () => {
      const handler = new RangeHandler();
      const emptyData = new Uint8Array(0);

      const result = handler.handleRange('bytes=0-10', emptyData);
      expect(result.status).toBe(416);
    });
  });

  describe('single byte file', () => {
    it('should handle single byte file', () => {
      const handler = new RangeHandler();
      const singleByte = new Uint8Array([0x42]);

      const result = handler.handleRange('bytes=0-0', singleByte);
      expect(result.status).toBe(206);
      expect(result.body).toEqual(new Uint8Array([0x42]));
    });

    it('should handle suffix range on single byte file', () => {
      const handler = new RangeHandler();
      const singleByte = new Uint8Array([0x42]);

      const result = handler.handleRange('bytes=-1', singleByte);
      expect(result.status).toBe(206);
      expect(result.body).toEqual(new Uint8Array([0x42]));
    });
  });

  describe('exact file size range', () => {
    it('should handle range exactly equal to file size', () => {
      const handler = new RangeHandler();
      const data = createTestData(100);

      const result = handler.handleRange('bytes=0-99', data);
      expect(result.status).toBe(206);
      expect(result.body?.byteLength).toBe(100);
    });
  });

  describe('boundary conditions', () => {
    it('should handle range ending at last byte', () => {
      const handler = new RangeHandler();
      const data = createTestData(100);

      const result = handler.handleRange('bytes=90-99', data);
      expect(result.status).toBe(206);
      expect(result.body?.byteLength).toBe(10);
      expect(result.headers['Content-Range']).toBe('bytes 90-99/100');
    });

    it('should handle range starting at first byte', () => {
      const handler = new RangeHandler();
      const data = createTestData(100);

      const result = handler.handleRange('bytes=0-9', data);
      expect(result.status).toBe(206);
      expect(result.body?.byteLength).toBe(10);
      expect(result.headers['Content-Range']).toBe('bytes 0-9/100');
    });
  });
});
