/**
 * Zone Map Generator Tests
 *
 * Zone maps in MongoLake:
 * - Track min/max values per row group for each column
 * - Enable predicate pushdown (skip row groups that can't match)
 * - Work with MongoDB document fields
 * - Handle nested field paths
 *
 * Zone maps are a critical optimization for query performance in analytical
 * workloads. By tracking column statistics at the row group level, we can
 * skip entire row groups that cannot possibly contain matching data.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  ZoneMapGenerator,
  ZoneMap,
  ZoneMapEntry,
  ColumnZoneMap,
  mergeZoneMaps,
  evaluatePredicate,
  type ZoneMapValue,
  type ZoneMapMetadata,
  type RangePredicate,
  type PredicateResult,
  serializeZoneMap,
  deserializeZoneMap,
} from '../../../src/parquet/zone-map.js';

// ============================================================================
// Test Fixtures
// ============================================================================

/**
 * Creates a sample row group with numeric data
 */
function createNumericRowGroup(values: (number | null)[]): Array<{ value: number | null }> {
  return values.map((value) => ({ value }));
}

/**
 * Creates a sample row group with string data
 */
function createStringRowGroup(values: (string | null)[]): Array<{ value: string | null }> {
  return values.map((value) => ({ value }));
}

/**
 * Creates a sample row group with date data
 */
function createDateRowGroup(values: (Date | null)[]): Array<{ value: Date | null }> {
  return values.map((value) => ({ value }));
}

/**
 * Creates a sample row group with nested documents
 */
function createNestedRowGroup(
  values: Array<{ address?: { city?: string; zip?: number } | null }>
): Array<{ address?: { city?: string; zip?: number } | null }> {
  return values;
}

/**
 * Creates a sample row group with mixed type values
 */
function createMixedTypeRowGroup(values: unknown[]): Array<{ value: unknown }> {
  return values.map((value) => ({ value }));
}

// ============================================================================
// Zone Map Generator - Numeric Fields
// ============================================================================

describe('ZoneMapGenerator - Numeric Fields', () => {
  let generator: ZoneMapGenerator;

  beforeEach(() => {
    generator = new ZoneMapGenerator();
  });

  describe('Track min/max for numeric fields', () => {
    it('should track min/max for integer values', () => {
      const rows = createNumericRowGroup([10, 25, 5, 42, 18]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap).toBeDefined();
      expect(zoneMap!.entries).toHaveLength(1);
      expect(zoneMap!.entries[0].min).toBe(5);
      expect(zoneMap!.entries[0].max).toBe(42);
    });

    it('should track min/max for floating point values', () => {
      const rows = createNumericRowGroup([3.14, 2.71, 1.41, 9.81, 6.28]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap).toBeDefined();
      expect(zoneMap!.entries[0].min).toBeCloseTo(1.41, 2);
      expect(zoneMap!.entries[0].max).toBeCloseTo(9.81, 2);
    });

    it('should track min/max for negative numbers', () => {
      const rows = createNumericRowGroup([-100, -50, 0, 25, -200]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].min).toBe(-200);
      expect(zoneMap!.entries[0].max).toBe(25);
    });

    it('should handle single value row group', () => {
      const rows = createNumericRowGroup([42]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].min).toBe(42);
      expect(zoneMap!.entries[0].max).toBe(42);
    });

    it('should track min/max for BigInt values', () => {
      const rows = [
        { value: BigInt('9007199254740991') }, // MAX_SAFE_INTEGER
        { value: BigInt('9007199254740993') },
        { value: BigInt('9007199254740990') },
      ];

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].min).toBe(BigInt('9007199254740990'));
      expect(zoneMap!.entries[0].max).toBe(BigInt('9007199254740993'));
    });

    it('should track multiple row groups independently', () => {
      const rows1 = createNumericRowGroup([10, 20, 30]);
      const rows2 = createNumericRowGroup([100, 200, 300]);

      generator.processRowGroup('rowGroup1', rows1, 'value');
      generator.processRowGroup('rowGroup2', rows2, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries).toHaveLength(2);
      expect(zoneMap!.entries[0].min).toBe(10);
      expect(zoneMap!.entries[0].max).toBe(30);
      expect(zoneMap!.entries[1].min).toBe(100);
      expect(zoneMap!.entries[1].max).toBe(300);
    });

    it('should handle Infinity values', () => {
      const rows = createNumericRowGroup([1, Infinity, -Infinity, 100]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].min).toBe(-Infinity);
      expect(zoneMap!.entries[0].max).toBe(Infinity);
    });

    it('should exclude NaN from min/max', () => {
      const rows = createNumericRowGroup([1, NaN, 100, NaN, 50]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].min).toBe(1);
      expect(zoneMap!.entries[0].max).toBe(100);
    });
  });
});

// ============================================================================
// Zone Map Generator - String Fields
// ============================================================================

describe('ZoneMapGenerator - String Fields', () => {
  let generator: ZoneMapGenerator;

  beforeEach(() => {
    generator = new ZoneMapGenerator();
  });

  describe('Track min/max for string fields', () => {
    it('should track min/max for simple strings', () => {
      const rows = createStringRowGroup(['charlie', 'alice', 'bob', 'david']);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap).toBeDefined();
      expect(zoneMap!.entries[0].min).toBe('alice');
      expect(zoneMap!.entries[0].max).toBe('david');
    });

    it('should use lexicographic comparison for strings', () => {
      const rows = createStringRowGroup(['z', 'A', 'a', 'Z']);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      // Lexicographic: 'A' < 'Z' < 'a' < 'z'
      expect(zoneMap!.entries[0].min).toBe('A');
      expect(zoneMap!.entries[0].max).toBe('z');
    });

    it('should handle empty strings', () => {
      const rows = createStringRowGroup(['hello', '', 'world']);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].min).toBe('');
      expect(zoneMap!.entries[0].max).toBe('world');
    });

    it('should handle unicode strings', () => {
      const rows = createStringRowGroup(['\u4e16\u754c', '\u4f60\u597d', 'hello', '\ud83c\udf0d']);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap).toBeDefined();
      // Unicode comparison should work correctly
      expect(typeof zoneMap!.entries[0].min).toBe('string');
      expect(typeof zoneMap!.entries[0].max).toBe('string');
    });

    it('should handle very long strings', () => {
      const longString = 'a'.repeat(10000);
      const rows = createStringRowGroup([longString, 'b', 'c']);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].min).toBe(longString);
      expect(zoneMap!.entries[0].max).toBe('c');
    });

    it('should handle strings with special characters', () => {
      const rows = createStringRowGroup(['hello\nworld', 'foo\tbar', 'test\0null']);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap).toBeDefined();
      expect(zoneMap!.entries).toHaveLength(1);
    });

    it('should track multiple row groups for strings', () => {
      const rows1 = createStringRowGroup(['alice', 'bob']);
      const rows2 = createStringRowGroup(['xavier', 'yolanda', 'zack']);

      generator.processRowGroup('rowGroup1', rows1, 'value');
      generator.processRowGroup('rowGroup2', rows2, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries).toHaveLength(2);
      expect(zoneMap!.entries[0].min).toBe('alice');
      expect(zoneMap!.entries[0].max).toBe('bob');
      expect(zoneMap!.entries[1].min).toBe('xavier');
      expect(zoneMap!.entries[1].max).toBe('zack');
    });
  });
});

// ============================================================================
// Zone Map Generator - Date Fields
// ============================================================================

describe('ZoneMapGenerator - Date Fields', () => {
  let generator: ZoneMapGenerator;

  beforeEach(() => {
    generator = new ZoneMapGenerator();
  });

  describe('Track min/max for date fields', () => {
    it('should track min/max for Date objects', () => {
      const dates = [
        new Date('2024-06-15'),
        new Date('2024-01-01'),
        new Date('2024-12-31'),
        new Date('2024-03-20'),
      ];
      const rows = createDateRowGroup(dates);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap).toBeDefined();
      expect(zoneMap!.entries[0].min).toEqual(new Date('2024-01-01'));
      expect(zoneMap!.entries[0].max).toEqual(new Date('2024-12-31'));
    });

    it('should track min/max for timestamps (milliseconds)', () => {
      const rows = [
        { value: 1704067200000 }, // 2024-01-01
        { value: 1735603200000 }, // 2024-12-31
        { value: 1718409600000 }, // 2024-06-15
      ];

      generator.processRowGroup('rowGroup1', rows, 'value', { treatAsTimestamp: true });
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].min).toBe(1704067200000);
      expect(zoneMap!.entries[0].max).toBe(1735603200000);
    });

    it('should handle epoch date', () => {
      const rows = createDateRowGroup([new Date(0), new Date('2024-01-01')]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].min).toEqual(new Date(0));
    });

    it('should handle pre-epoch dates', () => {
      const rows = createDateRowGroup([
        new Date('1969-07-20'),
        new Date('1970-01-01'),
        new Date('2024-01-01'),
      ]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].min).toEqual(new Date('1969-07-20'));
    });

    it('should handle far future dates', () => {
      const rows = createDateRowGroup([new Date('2024-01-01'), new Date('2099-12-31')]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].max).toEqual(new Date('2099-12-31'));
    });

    it('should track multiple row groups for dates', () => {
      const rows1 = createDateRowGroup([new Date('2024-01-01'), new Date('2024-06-30')]);
      const rows2 = createDateRowGroup([new Date('2024-07-01'), new Date('2024-12-31')]);

      generator.processRowGroup('rowGroup1', rows1, 'value');
      generator.processRowGroup('rowGroup2', rows2, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries).toHaveLength(2);
    });

    it('should handle ISO string dates', () => {
      const rows = [
        { value: '2024-01-01T00:00:00Z' },
        { value: '2024-12-31T23:59:59Z' },
        { value: '2024-06-15T12:00:00Z' },
      ];

      generator.processRowGroup('rowGroup1', rows, 'value', { parseAsDate: true });
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap).toBeDefined();
    });
  });
});

// ============================================================================
// Zone Map Generator - Null Handling
// ============================================================================

describe('ZoneMapGenerator - Null Handling', () => {
  let generator: ZoneMapGenerator;

  beforeEach(() => {
    generator = new ZoneMapGenerator();
  });

  describe('Handle null values correctly', () => {
    it('should exclude null from min/max calculation', () => {
      const rows = createNumericRowGroup([10, null, 5, null, 20]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].min).toBe(5);
      expect(zoneMap!.entries[0].max).toBe(20);
    });

    it('should handle all null row group', () => {
      const rows = createNumericRowGroup([null, null, null]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].min).toBeNull();
      expect(zoneMap!.entries[0].max).toBeNull();
      expect(zoneMap!.entries[0].allNull).toBe(true);
    });

    it('should track null count per row group', () => {
      const rows = createNumericRowGroup([10, null, 5, null, null, 20]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].nullCount).toBe(3);
    });

    it('should handle undefined as null', () => {
      const rows = [{ value: 10 }, { value: undefined }, { value: 20 }];

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].nullCount).toBe(1);
      expect(zoneMap!.entries[0].min).toBe(10);
      expect(zoneMap!.entries[0].max).toBe(20);
    });

    it('should handle missing fields as null', () => {
      const rows = [{ value: 10 }, {}, { value: 20 }];

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].nullCount).toBe(1);
    });

    it('should track null count for string fields', () => {
      const rows = createStringRowGroup(['alice', null, 'bob', null]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].nullCount).toBe(2);
      expect(zoneMap!.entries[0].min).toBe('alice');
      expect(zoneMap!.entries[0].max).toBe('bob');
    });

    it('should track null count for date fields', () => {
      const rows = createDateRowGroup([new Date('2024-01-01'), null, new Date('2024-12-31'), null]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].nullCount).toBe(2);
    });

    it('should track hasNull flag correctly', () => {
      const rowsWithNull = createNumericRowGroup([10, null, 20]);
      const rowsWithoutNull = createNumericRowGroup([10, 20, 30]);

      generator.processRowGroup('rowGroup1', rowsWithNull, 'value');
      generator.processRowGroup('rowGroup2', rowsWithoutNull, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].hasNull).toBe(true);
      expect(zoneMap!.entries[1].hasNull).toBe(false);
    });
  });
});

// ============================================================================
// Zone Map Generator - Nested Fields
// ============================================================================

describe('ZoneMapGenerator - Nested Fields', () => {
  let generator: ZoneMapGenerator;

  beforeEach(() => {
    generator = new ZoneMapGenerator();
  });

  describe('Track statistics for nested fields', () => {
    it('should track min/max for simple nested field (address.city)', () => {
      const rows = createNestedRowGroup([
        { address: { city: 'NYC', zip: 10001 } },
        { address: { city: 'LA', zip: 90001 } },
        { address: { city: 'Chicago', zip: 60601 } },
      ]);

      generator.processRowGroup('rowGroup1', rows, 'address.city');
      const zoneMap = generator.getZoneMap('address.city');

      expect(zoneMap).toBeDefined();
      expect(zoneMap!.entries[0].min).toBe('Chicago');
      expect(zoneMap!.entries[0].max).toBe('NYC');
    });

    it('should track min/max for nested numeric field (address.zip)', () => {
      const rows = createNestedRowGroup([
        { address: { city: 'NYC', zip: 10001 } },
        { address: { city: 'LA', zip: 90001 } },
        { address: { city: 'Chicago', zip: 60601 } },
      ]);

      generator.processRowGroup('rowGroup1', rows, 'address.zip');
      const zoneMap = generator.getZoneMap('address.zip');

      expect(zoneMap!.entries[0].min).toBe(10001);
      expect(zoneMap!.entries[0].max).toBe(90001);
    });

    it('should handle null nested object', () => {
      const rows = createNestedRowGroup([
        { address: { city: 'NYC', zip: 10001 } },
        { address: null },
        { address: { city: 'LA', zip: 90001 } },
      ]);

      generator.processRowGroup('rowGroup1', rows, 'address.city');
      const zoneMap = generator.getZoneMap('address.city');

      expect(zoneMap!.entries[0].nullCount).toBe(1);
      expect(zoneMap!.entries[0].min).toBe('LA');
      expect(zoneMap!.entries[0].max).toBe('NYC');
    });

    it('should handle missing nested field', () => {
      const rows = createNestedRowGroup([
        { address: { city: 'NYC', zip: 10001 } },
        { address: { zip: 90001 } }, // missing city
        { address: { city: 'LA', zip: 90001 } },
      ]);

      generator.processRowGroup('rowGroup1', rows, 'address.city');
      const zoneMap = generator.getZoneMap('address.city');

      expect(zoneMap!.entries[0].nullCount).toBe(1);
    });

    it('should handle deeply nested fields (a.b.c.d)', () => {
      const rows = [
        { a: { b: { c: { d: 100 } } } },
        { a: { b: { c: { d: 50 } } } },
        { a: { b: { c: { d: 200 } } } },
      ];

      generator.processRowGroup('rowGroup1', rows, 'a.b.c.d');
      const zoneMap = generator.getZoneMap('a.b.c.d');

      expect(zoneMap!.entries[0].min).toBe(50);
      expect(zoneMap!.entries[0].max).toBe(200);
    });

    it('should handle array field access with dot notation', () => {
      const rows = [
        { items: [{ price: 10 }, { price: 20 }] },
        { items: [{ price: 5 }, { price: 15 }] },
      ];

      // For arrays, we track min/max across all elements
      generator.processRowGroup('rowGroup1', rows, 'items.price', { flattenArrays: true });
      const zoneMap = generator.getZoneMap('items.price');

      expect(zoneMap!.entries[0].min).toBe(5);
      expect(zoneMap!.entries[0].max).toBe(20);
    });

    it('should track multiple nested fields independently', () => {
      const rows = createNestedRowGroup([
        { address: { city: 'NYC', zip: 10001 } },
        { address: { city: 'LA', zip: 90001 } },
      ]);

      generator.processRowGroup('rowGroup1', rows, 'address.city');
      generator.processRowGroup('rowGroup1', rows, 'address.zip');

      const cityZoneMap = generator.getZoneMap('address.city');
      const zipZoneMap = generator.getZoneMap('address.zip');

      expect(cityZoneMap).toBeDefined();
      expect(zipZoneMap).toBeDefined();
      expect(cityZoneMap!.fieldPath).toBe('address.city');
      expect(zipZoneMap!.fieldPath).toBe('address.zip');
    });

    it('should handle escaped dots in field names', () => {
      const rows = [{ 'field.with.dots': 10 }, { 'field.with.dots': 20 }];

      generator.processRowGroup('rowGroup1', rows, 'field\\.with\\.dots');
      const zoneMap = generator.getZoneMap('field\\.with\\.dots');

      expect(zoneMap!.entries[0].min).toBe(10);
      expect(zoneMap!.entries[0].max).toBe(20);
    });
  });
});

// ============================================================================
// Zone Map Merging
// ============================================================================

describe('ZoneMap - Merge Operations', () => {
  describe('Merge zone maps when compacting blocks', () => {
    it('should merge two zone maps with numeric fields', () => {
      const map1: ZoneMap = {
        fieldPath: 'value',
        fieldType: 'number',
        entries: [
          {
            rowGroupId: 'rg1',
            min: 10,
            max: 100,
            nullCount: 2,
            hasNull: true,
            allNull: false,
            rowCount: 50,
          },
        ],
      };

      const map2: ZoneMap = {
        fieldPath: 'value',
        fieldType: 'number',
        entries: [
          {
            rowGroupId: 'rg2',
            min: 5,
            max: 200,
            nullCount: 1,
            hasNull: true,
            allNull: false,
            rowCount: 50,
          },
        ],
      };

      const merged = mergeZoneMaps(map1, map2);

      expect(merged.entries).toHaveLength(1);
      expect(merged.entries[0].min).toBe(5);
      expect(merged.entries[0].max).toBe(200);
      expect(merged.entries[0].nullCount).toBe(3);
      expect(merged.entries[0].rowCount).toBe(100);
    });

    it('should merge zone maps with string fields', () => {
      const map1: ZoneMap = {
        fieldPath: 'name',
        fieldType: 'string',
        entries: [
          {
            rowGroupId: 'rg1',
            min: 'charlie',
            max: 'zack',
            nullCount: 0,
            hasNull: false,
            allNull: false,
            rowCount: 25,
          },
        ],
      };

      const map2: ZoneMap = {
        fieldPath: 'name',
        fieldType: 'string',
        entries: [
          {
            rowGroupId: 'rg2',
            min: 'alice',
            max: 'xavier',
            nullCount: 0,
            hasNull: false,
            allNull: false,
            rowCount: 25,
          },
        ],
      };

      const merged = mergeZoneMaps(map1, map2);

      expect(merged.entries[0].min).toBe('alice');
      expect(merged.entries[0].max).toBe('zack');
    });

    it('should merge zone maps with date fields', () => {
      const date1 = new Date('2024-01-01');
      const date2 = new Date('2024-06-30');
      const date3 = new Date('2024-07-01');
      const date4 = new Date('2024-12-31');

      const map1: ZoneMap = {
        fieldPath: 'createdAt',
        fieldType: 'date',
        entries: [
          {
            rowGroupId: 'rg1',
            min: date1,
            max: date2,
            nullCount: 0,
            hasNull: false,
            allNull: false,
            rowCount: 50,
          },
        ],
      };

      const map2: ZoneMap = {
        fieldPath: 'createdAt',
        fieldType: 'date',
        entries: [
          {
            rowGroupId: 'rg2',
            min: date3,
            max: date4,
            nullCount: 0,
            hasNull: false,
            allNull: false,
            rowCount: 50,
          },
        ],
      };

      const merged = mergeZoneMaps(map1, map2);

      expect(merged.entries[0].min).toEqual(date1);
      expect(merged.entries[0].max).toEqual(date4);
    });

    it('should merge zone maps with all-null entries', () => {
      const map1: ZoneMap = {
        fieldPath: 'value',
        fieldType: 'number',
        entries: [
          {
            rowGroupId: 'rg1',
            min: null,
            max: null,
            nullCount: 10,
            hasNull: true,
            allNull: true,
            rowCount: 10,
          },
        ],
      };

      const map2: ZoneMap = {
        fieldPath: 'value',
        fieldType: 'number',
        entries: [
          {
            rowGroupId: 'rg2',
            min: 5,
            max: 100,
            nullCount: 2,
            hasNull: true,
            allNull: false,
            rowCount: 20,
          },
        ],
      };

      const merged = mergeZoneMaps(map1, map2);

      expect(merged.entries[0].min).toBe(5);
      expect(merged.entries[0].max).toBe(100);
      expect(merged.entries[0].nullCount).toBe(12);
      expect(merged.entries[0].allNull).toBe(false);
    });

    it('should throw error when merging incompatible field paths', () => {
      const map1: ZoneMap = {
        fieldPath: 'field1',
        fieldType: 'number',
        entries: [],
      };

      const map2: ZoneMap = {
        fieldPath: 'field2',
        fieldType: 'number',
        entries: [],
      };

      expect(() => mergeZoneMaps(map1, map2)).toThrow();
    });

    it('should handle merging multiple zone maps', () => {
      const maps: ZoneMap[] = [
        {
          fieldPath: 'value',
          fieldType: 'number',
          entries: [
            { rowGroupId: 'rg1', min: 10, max: 20, nullCount: 0, hasNull: false, allNull: false, rowCount: 10 },
          ],
        },
        {
          fieldPath: 'value',
          fieldType: 'number',
          entries: [
            { rowGroupId: 'rg2', min: 30, max: 40, nullCount: 1, hasNull: true, allNull: false, rowCount: 10 },
          ],
        },
        {
          fieldPath: 'value',
          fieldType: 'number',
          entries: [
            { rowGroupId: 'rg3', min: 5, max: 100, nullCount: 0, hasNull: false, allNull: false, rowCount: 10 },
          ],
        },
      ];

      const merged = maps.reduce((acc, map) => mergeZoneMaps(acc, map));

      expect(merged.entries[0].min).toBe(5);
      expect(merged.entries[0].max).toBe(100);
      expect(merged.entries[0].nullCount).toBe(1);
      expect(merged.entries[0].rowCount).toBe(30);
    });

    it('should preserve row group granularity when not merging', () => {
      const map1: ZoneMap = {
        fieldPath: 'value',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg1', min: 10, max: 20, nullCount: 0, hasNull: false, allNull: false, rowCount: 10 },
        ],
      };

      const map2: ZoneMap = {
        fieldPath: 'value',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg2', min: 30, max: 40, nullCount: 0, hasNull: false, allNull: false, rowCount: 10 },
        ],
      };

      const combined = mergeZoneMaps(map1, map2, { preserveRowGroups: true });

      expect(combined.entries).toHaveLength(2);
      expect(combined.entries[0].rowGroupId).toBe('rg1');
      expect(combined.entries[1].rowGroupId).toBe('rg2');
    });
  });
});

// ============================================================================
// Zone Map Predicate Evaluation
// ============================================================================

describe('ZoneMap - Predicate Evaluation', () => {
  describe('Evaluate range predicates against zone map', () => {
    it('should return MATCH for $eq when value is within range', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'age',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg1', min: 18, max: 65, nullCount: 0, hasNull: false, allNull: false, rowCount: 100 },
        ],
      };

      const predicate: RangePredicate = { op: '$eq', value: 30 };
      const result = evaluatePredicate(zoneMap, 'rg1', predicate);

      expect(result).toBe('MATCH');
    });

    it('should return NO_MATCH for $eq when value is outside range', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'age',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg1', min: 18, max: 65, nullCount: 0, hasNull: false, allNull: false, rowCount: 100 },
        ],
      };

      const predicate: RangePredicate = { op: '$eq', value: 100 };
      const result = evaluatePredicate(zoneMap, 'rg1', predicate);

      expect(result).toBe('NO_MATCH');
    });

    it('should return MATCH for $gt when min < value', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'price',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg1', min: 10, max: 100, nullCount: 0, hasNull: false, allNull: false, rowCount: 100 },
        ],
      };

      const predicate: RangePredicate = { op: '$gt', value: 50 };
      const result = evaluatePredicate(zoneMap, 'rg1', predicate);

      expect(result).toBe('MATCH');
    });

    it('should return NO_MATCH for $gt when max <= value', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'price',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg1', min: 10, max: 100, nullCount: 0, hasNull: false, allNull: false, rowCount: 100 },
        ],
      };

      const predicate: RangePredicate = { op: '$gt', value: 100 };
      const result = evaluatePredicate(zoneMap, 'rg1', predicate);

      expect(result).toBe('NO_MATCH');
    });

    it('should return MATCH for $gte when min <= value', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'price',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg1', min: 10, max: 100, nullCount: 0, hasNull: false, allNull: false, rowCount: 100 },
        ],
      };

      const predicate: RangePredicate = { op: '$gte', value: 100 };
      const result = evaluatePredicate(zoneMap, 'rg1', predicate);

      expect(result).toBe('MATCH');
    });

    it('should return NO_MATCH for $gte when max < value', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'price',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg1', min: 10, max: 100, nullCount: 0, hasNull: false, allNull: false, rowCount: 100 },
        ],
      };

      const predicate: RangePredicate = { op: '$gte', value: 101 };
      const result = evaluatePredicate(zoneMap, 'rg1', predicate);

      expect(result).toBe('NO_MATCH');
    });

    it('should return MATCH for $lt when max > value', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'price',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg1', min: 10, max: 100, nullCount: 0, hasNull: false, allNull: false, rowCount: 100 },
        ],
      };

      const predicate: RangePredicate = { op: '$lt', value: 50 };
      const result = evaluatePredicate(zoneMap, 'rg1', predicate);

      expect(result).toBe('MATCH');
    });

    it('should return NO_MATCH for $lt when min >= value', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'price',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg1', min: 10, max: 100, nullCount: 0, hasNull: false, allNull: false, rowCount: 100 },
        ],
      };

      const predicate: RangePredicate = { op: '$lt', value: 10 };
      const result = evaluatePredicate(zoneMap, 'rg1', predicate);

      expect(result).toBe('NO_MATCH');
    });

    it('should return MATCH for $lte when max >= value', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'price',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg1', min: 10, max: 100, nullCount: 0, hasNull: false, allNull: false, rowCount: 100 },
        ],
      };

      const predicate: RangePredicate = { op: '$lte', value: 10 };
      const result = evaluatePredicate(zoneMap, 'rg1', predicate);

      expect(result).toBe('MATCH');
    });

    it('should return NO_MATCH for $lte when min > value', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'price',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg1', min: 10, max: 100, nullCount: 0, hasNull: false, allNull: false, rowCount: 100 },
        ],
      };

      const predicate: RangePredicate = { op: '$lte', value: 9 };
      const result = evaluatePredicate(zoneMap, 'rg1', predicate);

      expect(result).toBe('NO_MATCH');
    });

    it('should return MATCH for $in when any value is in range', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'status',
        fieldType: 'string',
        entries: [
          { rowGroupId: 'rg1', min: 'active', max: 'pending', nullCount: 0, hasNull: false, allNull: false, rowCount: 100 },
        ],
      };

      const predicate: RangePredicate = { op: '$in', value: ['active', 'inactive', 'deleted'] };
      const result = evaluatePredicate(zoneMap, 'rg1', predicate);

      expect(result).toBe('MATCH');
    });

    it('should return NO_MATCH for $in when no values in range', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'age',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg1', min: 18, max: 30, nullCount: 0, hasNull: false, allNull: false, rowCount: 100 },
        ],
      };

      const predicate: RangePredicate = { op: '$in', value: [50, 60, 70] };
      const result = evaluatePredicate(zoneMap, 'rg1', predicate);

      expect(result).toBe('NO_MATCH');
    });

    it('should evaluate string predicates correctly', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'name',
        fieldType: 'string',
        entries: [
          { rowGroupId: 'rg1', min: 'alice', max: 'charlie', nullCount: 0, hasNull: false, allNull: false, rowCount: 100 },
        ],
      };

      expect(evaluatePredicate(zoneMap, 'rg1', { op: '$eq', value: 'bob' })).toBe('MATCH');
      expect(evaluatePredicate(zoneMap, 'rg1', { op: '$eq', value: 'david' })).toBe('NO_MATCH');
      expect(evaluatePredicate(zoneMap, 'rg1', { op: '$lt', value: 'bob' })).toBe('MATCH');
      expect(evaluatePredicate(zoneMap, 'rg1', { op: '$gt', value: 'bob' })).toBe('MATCH');
    });

    it('should evaluate date predicates correctly', () => {
      const minDate = new Date('2024-01-01');
      const maxDate = new Date('2024-06-30');

      const zoneMap: ZoneMap = {
        fieldPath: 'createdAt',
        fieldType: 'date',
        entries: [
          { rowGroupId: 'rg1', min: minDate, max: maxDate, nullCount: 0, hasNull: false, allNull: false, rowCount: 100 },
        ],
      };

      expect(evaluatePredicate(zoneMap, 'rg1', { op: '$eq', value: new Date('2024-03-15') })).toBe('MATCH');
      expect(evaluatePredicate(zoneMap, 'rg1', { op: '$eq', value: new Date('2024-12-31') })).toBe('NO_MATCH');
    });

    it('should return UNKNOWN for all-null row groups', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'value',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg1', min: null, max: null, nullCount: 100, hasNull: true, allNull: true, rowCount: 100 },
        ],
      };

      const predicate: RangePredicate = { op: '$eq', value: 50 };
      const result = evaluatePredicate(zoneMap, 'rg1', predicate);

      expect(result).toBe('UNKNOWN');
    });

    it('should handle $ne predicate', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'value',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg1', min: 10, max: 10, nullCount: 0, hasNull: false, allNull: false, rowCount: 100 },
        ],
      };

      // When min == max, we can definitively say $ne doesn't match
      expect(evaluatePredicate(zoneMap, 'rg1', { op: '$ne', value: 10 })).toBe('NO_MATCH');
      expect(evaluatePredicate(zoneMap, 'rg1', { op: '$ne', value: 20 })).toBe('MATCH');
    });
  });
});

// ============================================================================
// Zone Map - Mixed Types
// ============================================================================

describe('ZoneMapGenerator - Mixed Types', () => {
  let generator: ZoneMapGenerator;

  beforeEach(() => {
    generator = new ZoneMapGenerator();
  });

  describe('Handle mixed types in same field', () => {
    it('should handle numbers and strings in same field', () => {
      const rows = createMixedTypeRowGroup([10, 'hello', 20, 'world']);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      // Mixed types should be tracked with type information
      expect(zoneMap).toBeDefined();
      expect(zoneMap!.entries[0].hasMixedTypes).toBe(true);
    });

    it('should track min/max per type for mixed fields', () => {
      const rows = createMixedTypeRowGroup([10, 'alpha', 20, 'beta', 5]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].numericMin).toBe(5);
      expect(zoneMap!.entries[0].numericMax).toBe(20);
      expect(zoneMap!.entries[0].stringMin).toBe('alpha');
      expect(zoneMap!.entries[0].stringMax).toBe('beta');
    });

    it('should handle booleans mixed with other types', () => {
      const rows = createMixedTypeRowGroup([true, 10, false, 'hello']);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].hasMixedTypes).toBe(true);
      expect(zoneMap!.entries[0].hasBoolean).toBe(true);
    });

    it('should handle objects mixed with primitives', () => {
      const rows = createMixedTypeRowGroup([{ nested: true }, 10, 'hello']);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].hasMixedTypes).toBe(true);
      expect(zoneMap!.entries[0].hasObject).toBe(true);
    });

    it('should handle arrays mixed with primitives', () => {
      const rows = createMixedTypeRowGroup([[1, 2, 3], 10, 'hello']);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].hasMixedTypes).toBe(true);
      expect(zoneMap!.entries[0].hasArray).toBe(true);
    });

    it('should track type counts for mixed fields', () => {
      const rows = createMixedTypeRowGroup([10, 20, 'a', 'b', 'c', true]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].typeCounts).toBeDefined();
      expect(zoneMap!.entries[0].typeCounts!.number).toBe(2);
      expect(zoneMap!.entries[0].typeCounts!.string).toBe(3);
      expect(zoneMap!.entries[0].typeCounts!.boolean).toBe(1);
    });
  });
});

// ============================================================================
// Zone Map - Null Count Per Row Group
// ============================================================================

describe('ZoneMapGenerator - Null Count Tracking', () => {
  let generator: ZoneMapGenerator;

  beforeEach(() => {
    generator = new ZoneMapGenerator();
  });

  describe('Track null count per row group', () => {
    it('should track null count accurately for numeric fields', () => {
      const rows = createNumericRowGroup([1, null, 2, null, null, 3, null]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].nullCount).toBe(4);
    });

    it('should track null count accurately for string fields', () => {
      const rows = createStringRowGroup(['a', null, 'b', null]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].nullCount).toBe(2);
    });

    it('should track null count accurately for date fields', () => {
      const rows = createDateRowGroup([new Date(), null, null, new Date()]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].nullCount).toBe(2);
    });

    it('should track null count per row group independently', () => {
      const rows1 = createNumericRowGroup([1, null, 2]); // 1 null
      const rows2 = createNumericRowGroup([null, null, 3]); // 2 nulls
      const rows3 = createNumericRowGroup([4, 5, 6]); // 0 nulls

      generator.processRowGroup('rg1', rows1, 'value');
      generator.processRowGroup('rg2', rows2, 'value');
      generator.processRowGroup('rg3', rows3, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].nullCount).toBe(1);
      expect(zoneMap!.entries[1].nullCount).toBe(2);
      expect(zoneMap!.entries[2].nullCount).toBe(0);
    });

    it('should calculate null ratio correctly', () => {
      const rows = createNumericRowGroup([1, null, 2, null]); // 50% null

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      const entry = zoneMap!.entries[0];
      const nullRatio = entry.nullCount / entry.rowCount;
      expect(nullRatio).toBe(0.5);
    });

    it('should track total row count with null count', () => {
      const rows = createNumericRowGroup([1, null, 2, null, 3]);

      generator.processRowGroup('rowGroup1', rows, 'value');
      const zoneMap = generator.getZoneMap('value');

      expect(zoneMap!.entries[0].rowCount).toBe(5);
      expect(zoneMap!.entries[0].nullCount).toBe(2);
    });
  });
});

// ============================================================================
// Zone Map Serialization/Deserialization
// ============================================================================

describe('ZoneMap - Serialization', () => {
  describe('Serialize/deserialize zone map metadata', () => {
    it('should serialize zone map to binary format', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'value',
        fieldType: 'number',
        entries: [
          {
            rowGroupId: 'rg1',
            min: 10,
            max: 100,
            nullCount: 5,
            hasNull: true,
            allNull: false,
            rowCount: 50,
          },
        ],
      };

      const serialized = serializeZoneMap(zoneMap);

      expect(serialized).toBeInstanceOf(Uint8Array);
      expect(serialized.byteLength).toBeGreaterThan(0);
    });

    it('should deserialize zone map from binary format', () => {
      const original: ZoneMap = {
        fieldPath: 'value',
        fieldType: 'number',
        entries: [
          {
            rowGroupId: 'rg1',
            min: 10,
            max: 100,
            nullCount: 5,
            hasNull: true,
            allNull: false,
            rowCount: 50,
          },
        ],
      };

      const serialized = serializeZoneMap(original);
      const deserialized = deserializeZoneMap(serialized);

      expect(deserialized.fieldPath).toBe(original.fieldPath);
      expect(deserialized.fieldType).toBe(original.fieldType);
      expect(deserialized.entries[0].min).toBe(original.entries[0].min);
      expect(deserialized.entries[0].max).toBe(original.entries[0].max);
      expect(deserialized.entries[0].nullCount).toBe(original.entries[0].nullCount);
    });

    it('should serialize/deserialize string zone maps', () => {
      const original: ZoneMap = {
        fieldPath: 'name',
        fieldType: 'string',
        entries: [
          {
            rowGroupId: 'rg1',
            min: 'alice',
            max: 'zack',
            nullCount: 2,
            hasNull: true,
            allNull: false,
            rowCount: 100,
          },
        ],
      };

      const serialized = serializeZoneMap(original);
      const deserialized = deserializeZoneMap(serialized);

      expect(deserialized.entries[0].min).toBe('alice');
      expect(deserialized.entries[0].max).toBe('zack');
    });

    it('should serialize/deserialize date zone maps', () => {
      const minDate = new Date('2024-01-01');
      const maxDate = new Date('2024-12-31');

      const original: ZoneMap = {
        fieldPath: 'createdAt',
        fieldType: 'date',
        entries: [
          {
            rowGroupId: 'rg1',
            min: minDate,
            max: maxDate,
            nullCount: 0,
            hasNull: false,
            allNull: false,
            rowCount: 100,
          },
        ],
      };

      const serialized = serializeZoneMap(original);
      const deserialized = deserializeZoneMap(serialized);

      expect(deserialized.entries[0].min).toEqual(minDate);
      expect(deserialized.entries[0].max).toEqual(maxDate);
    });

    it('should serialize/deserialize multiple entries', () => {
      const original: ZoneMap = {
        fieldPath: 'value',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg1', min: 10, max: 20, nullCount: 0, hasNull: false, allNull: false, rowCount: 10 },
          { rowGroupId: 'rg2', min: 30, max: 40, nullCount: 1, hasNull: true, allNull: false, rowCount: 10 },
          { rowGroupId: 'rg3', min: 50, max: 60, nullCount: 0, hasNull: false, allNull: false, rowCount: 10 },
        ],
      };

      const serialized = serializeZoneMap(original);
      const deserialized = deserializeZoneMap(serialized);

      expect(deserialized.entries).toHaveLength(3);
      expect(deserialized.entries[0].rowGroupId).toBe('rg1');
      expect(deserialized.entries[1].rowGroupId).toBe('rg2');
      expect(deserialized.entries[2].rowGroupId).toBe('rg3');
    });

    it('should serialize/deserialize all-null entries', () => {
      const original: ZoneMap = {
        fieldPath: 'value',
        fieldType: 'number',
        entries: [
          {
            rowGroupId: 'rg1',
            min: null,
            max: null,
            nullCount: 100,
            hasNull: true,
            allNull: true,
            rowCount: 100,
          },
        ],
      };

      const serialized = serializeZoneMap(original);
      const deserialized = deserializeZoneMap(serialized);

      expect(deserialized.entries[0].min).toBeNull();
      expect(deserialized.entries[0].max).toBeNull();
      expect(deserialized.entries[0].allNull).toBe(true);
    });

    it('should serialize/deserialize BigInt values', () => {
      const original: ZoneMap = {
        fieldPath: 'bigValue',
        fieldType: 'bigint',
        entries: [
          {
            rowGroupId: 'rg1',
            min: BigInt('9007199254740991'),
            max: BigInt('9007199254740999'),
            nullCount: 0,
            hasNull: false,
            allNull: false,
            rowCount: 10,
          },
        ],
      };

      const serialized = serializeZoneMap(original);
      const deserialized = deserializeZoneMap(serialized);

      expect(deserialized.entries[0].min).toBe(BigInt('9007199254740991'));
      expect(deserialized.entries[0].max).toBe(BigInt('9007199254740999'));
    });

    it('should serialize/deserialize mixed type zone maps', () => {
      const original: ZoneMap = {
        fieldPath: 'mixed',
        fieldType: 'mixed',
        entries: [
          {
            rowGroupId: 'rg1',
            min: null,
            max: null,
            nullCount: 0,
            hasNull: false,
            allNull: false,
            rowCount: 10,
            hasMixedTypes: true,
            numericMin: 10,
            numericMax: 100,
            stringMin: 'alpha',
            stringMax: 'zeta',
          },
        ],
      };

      const serialized = serializeZoneMap(original);
      const deserialized = deserializeZoneMap(serialized);

      expect(deserialized.entries[0].hasMixedTypes).toBe(true);
      expect(deserialized.entries[0].numericMin).toBe(10);
      expect(deserialized.entries[0].numericMax).toBe(100);
      expect(deserialized.entries[0].stringMin).toBe('alpha');
      expect(deserialized.entries[0].stringMax).toBe('zeta');
    });

    it('should produce compact serialization', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'value',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg1', min: 10, max: 100, nullCount: 0, hasNull: false, allNull: false, rowCount: 100 },
        ],
      };

      const serialized = serializeZoneMap(zoneMap);

      // Should be reasonably compact
      expect(serialized.byteLength).toBeLessThan(200);
    });

    it('should handle nested field paths in serialization', () => {
      const original: ZoneMap = {
        fieldPath: 'address.city.name',
        fieldType: 'string',
        entries: [
          {
            rowGroupId: 'rg1',
            min: 'Boston',
            max: 'NYC',
            nullCount: 5,
            hasNull: true,
            allNull: false,
            rowCount: 100,
          },
        ],
      };

      const serialized = serializeZoneMap(original);
      const deserialized = deserializeZoneMap(serialized);

      expect(deserialized.fieldPath).toBe('address.city.name');
    });

    it('should serialize to JSON format', () => {
      const zoneMap: ZoneMap = {
        fieldPath: 'value',
        fieldType: 'number',
        entries: [
          { rowGroupId: 'rg1', min: 10, max: 100, nullCount: 5, hasNull: true, allNull: false, rowCount: 50 },
        ],
      };

      const serialized = serializeZoneMap(zoneMap, { format: 'json' });
      const json = JSON.parse(new TextDecoder().decode(serialized));

      expect(json.fieldPath).toBe('value');
      expect(json.entries[0].min).toBe(10);
    });

    it('should validate zone map on deserialization', () => {
      const invalidData = new Uint8Array([0, 1, 2, 3, 4, 5]);

      expect(() => deserializeZoneMap(invalidData)).toThrow();
    });
  });
});

// ============================================================================
// Zone Map Generator - Complete Integration
// ============================================================================

describe('ZoneMapGenerator - Integration', () => {
  it('should track multiple columns across multiple row groups', () => {
    const generator = new ZoneMapGenerator();

    const rows1 = [
      { name: 'Alice', age: 30, active: true },
      { name: 'Bob', age: 25, active: false },
    ];

    const rows2 = [
      { name: 'Charlie', age: 35, active: true },
      { name: 'Diana', age: 28, active: true },
    ];

    generator.processRowGroup('rg1', rows1, 'name');
    generator.processRowGroup('rg1', rows1, 'age');
    generator.processRowGroup('rg2', rows2, 'name');
    generator.processRowGroup('rg2', rows2, 'age');

    const nameZoneMap = generator.getZoneMap('name');
    const ageZoneMap = generator.getZoneMap('age');

    expect(nameZoneMap!.entries).toHaveLength(2);
    expect(ageZoneMap!.entries).toHaveLength(2);

    expect(nameZoneMap!.entries[0].min).toBe('Alice');
    expect(nameZoneMap!.entries[0].max).toBe('Bob');
    expect(ageZoneMap!.entries[0].min).toBe(25);
    expect(ageZoneMap!.entries[0].max).toBe(30);
  });

  it('should generate zone map metadata for Parquet footer', () => {
    const generator = new ZoneMapGenerator();

    const rows = [
      { _id: 'doc1', value: 100, name: 'Alice' },
      { _id: 'doc2', value: 200, name: 'Bob' },
      { _id: 'doc3', value: 150, name: null },
    ];

    generator.processRowGroup('rg1', rows, '_id');
    generator.processRowGroup('rg1', rows, 'value');
    generator.processRowGroup('rg1', rows, 'name');

    const metadata: ZoneMapMetadata = generator.getMetadata();

    expect(metadata.columns).toBeDefined();
    expect(Object.keys(metadata.columns)).toContain('_id');
    expect(Object.keys(metadata.columns)).toContain('value');
    expect(Object.keys(metadata.columns)).toContain('name');
  });

  it('should support batch processing of documents', () => {
    const generator = new ZoneMapGenerator();

    const documents = Array.from({ length: 1000 }, (_, i) => ({
      _id: `doc${i}`,
      value: Math.floor(Math.random() * 1000),
      name: `user${i % 100}`,
    }));

    generator.processRowGroupBatch('rg1', documents, ['_id', 'value', 'name']);

    const valueZoneMap = generator.getZoneMap('value');

    expect(valueZoneMap).toBeDefined();
    expect(valueZoneMap!.entries[0].rowCount).toBe(1000);
  });
});
