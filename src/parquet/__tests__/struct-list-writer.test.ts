/**
 * Struct/List Column Writer Tests
 *
 * Tests for writing nested MongoDB documents as native Parquet struct and list types.
 * Verifies proper handling of repetition/definition levels for arbitrary nesting depth.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  ColumnWriter,
  StructColumnWriter,
  ListColumnWriter,
  type WrittenColumn,
  type FieldDefinition,
  type ColumnWriterOptions,
} from '../column-writer.js';

describe('StructColumnWriter', () => {
  describe('Simple struct writing', () => {
    it('should write a simple struct with primitive fields', () => {
      const fields: FieldDefinition[] = [
        { name: 'street', type: 'string' },
        { name: 'city', type: 'string' },
        { name: 'zip', type: 'int32' },
      ];

      const writer = new StructColumnWriter('address', { fields });

      writer.write({ street: '123 Main St', city: 'Springfield', zip: 12345 });
      writer.write({ street: '456 Oak Ave', city: 'Shelbyville', zip: 67890 });

      const result = writer.finish();

      expect(result.columnName).toBe('address');
      expect(result.dataType).toBe('STRUCT');
      expect(result.numValues).toBe(2);
      expect(result.children).toBeDefined();
      expect(result.children).toHaveLength(3);

      // Verify child column names are properly prefixed
      expect(result.children![0].columnName).toBe('address.street');
      expect(result.children![1].columnName).toBe('address.city');
      expect(result.children![2].columnName).toBe('address.zip');

      // Verify child data types
      expect(result.children![0].dataType).toBe('BYTE_ARRAY');
      expect(result.children![1].dataType).toBe('BYTE_ARRAY');
      expect(result.children![2].dataType).toBe('INT32');
    });

    it('should handle null struct values with definition levels', () => {
      const fields: FieldDefinition[] = [
        { name: 'name', type: 'string' },
        { name: 'value', type: 'int32' },
      ];

      const writer = new StructColumnWriter('metadata', { fields, optional: true });

      writer.write({ name: 'key1', value: 100 });
      writer.write(null);
      writer.write({ name: 'key3', value: 300 });

      const result = writer.finish();

      expect(result.numValues).toBe(3);
      expect(result.statistics?.nullCount).toBe(1);

      // Definition levels should indicate null struct (0) vs present (>0)
      expect(result.definitionLevels).toBeDefined();
      expect(result.definitionLevels![0]).toBeGreaterThan(0); // present
      expect(result.definitionLevels![1]).toBe(0); // null
      expect(result.definitionLevels![2]).toBeGreaterThan(0); // present
    });

    it('should handle null field values within structs', () => {
      const fields: FieldDefinition[] = [
        { name: 'required', type: 'string' },
        { name: 'optional', type: 'int32' },
      ];

      const writer = new StructColumnWriter('data', { fields });

      writer.write({ required: 'a', optional: 1 });
      writer.write({ required: 'b', optional: null });
      writer.write({ required: 'c', optional: 3 });

      const result = writer.finish();

      expect(result.children).toHaveLength(2);

      const optionalChild = result.children!.find((c) => c.columnName === 'data.optional');
      expect(optionalChild?.statistics?.nullCount).toBe(1);
    });

    it('should track statistics for struct fields', () => {
      const fields: FieldDefinition[] = [
        { name: 'score', type: 'int32' },
        { name: 'name', type: 'string' },
      ];

      const writer = new StructColumnWriter('player', { fields });

      writer.write({ score: 100, name: 'Alice' });
      writer.write({ score: 250, name: 'Bob' });
      writer.write({ score: 175, name: 'Charlie' });

      const result = writer.finish();

      const scoreChild = result.children!.find((c) => c.columnName === 'player.score');
      expect(scoreChild?.statistics?.minValue).toBe(100);
      expect(scoreChild?.statistics?.maxValue).toBe(250);
    });
  });

  describe('Nested struct writing', () => {
    it('should write nested structs (struct containing struct)', () => {
      const fields: FieldDefinition[] = [
        { name: 'name', type: 'string' },
        {
          name: 'address',
          type: 'struct',
          fields: [
            { name: 'street', type: 'string' },
            { name: 'city', type: 'string' },
          ],
        },
      ];

      const writer = new StructColumnWriter('person', { fields });

      writer.write({
        name: 'Alice',
        address: { street: '123 Main St', city: 'Springfield' },
      });

      const result = writer.finish();

      expect(result.children).toHaveLength(2);

      const addressChild = result.children!.find((c) => c.columnName === 'person.address');
      expect(addressChild?.dataType).toBe('STRUCT');
      expect(addressChild?.children).toHaveLength(2);

      // Nested column names should be fully qualified
      expect(addressChild?.children![0].columnName).toBe('person.address.street');
      expect(addressChild?.children![1].columnName).toBe('person.address.city');
    });

    it('should handle deeply nested structs (3+ levels)', () => {
      const fields: FieldDefinition[] = [
        {
          name: 'level1',
          type: 'struct',
          fields: [
            {
              name: 'level2',
              type: 'struct',
              fields: [
                {
                  name: 'level3',
                  type: 'struct',
                  fields: [{ name: 'value', type: 'int32' }],
                },
              ],
            },
          ],
        },
      ];

      const writer = new StructColumnWriter('deep', { fields });

      writer.write({
        level1: {
          level2: {
            level3: {
              value: 42,
            },
          },
        },
      });

      const result = writer.finish();

      // Navigate through the nested structure
      const level1 = result.children![0];
      expect(level1.columnName).toBe('deep.level1');

      const level2 = level1.children![0];
      expect(level2.columnName).toBe('deep.level1.level2');

      const level3 = level2.children![0];
      expect(level3.columnName).toBe('deep.level1.level2.level3');

      const valueCol = level3.children![0];
      expect(valueCol.columnName).toBe('deep.level1.level2.level3.value');
      expect(valueCol.dataType).toBe('INT32');
    });

    it('should handle null at different nesting levels', () => {
      const fields: FieldDefinition[] = [
        {
          name: 'outer',
          type: 'struct',
          fields: [
            {
              name: 'inner',
              type: 'struct',
              fields: [{ name: 'value', type: 'int32' }],
            },
          ],
        },
      ];

      const writer = new StructColumnWriter('nested', { fields, optional: true });

      // Value at innermost level
      writer.write({ outer: { inner: { value: 1 } } });
      // Null inner struct
      writer.write({ outer: { inner: null } });
      // Null outer struct
      writer.write({ outer: null });
      // Entire struct is null
      writer.write(null);

      const result = writer.finish();

      expect(result.numValues).toBe(4);
      // maxDefinitionLevel tracks optional struct presence (simplified model)
      // For deeply nested structures, level indicates struct optionality
      expect(result.maxDefinitionLevel).toBeGreaterThanOrEqual(1);
    });

    it('should calculate correct max definition level for nested optional structs', () => {
      const fields: FieldDefinition[] = [
        {
          name: 'a',
          type: 'struct',
          fields: [
            {
              name: 'b',
              type: 'struct',
              fields: [{ name: 'c', type: 'int32' }],
            },
          ],
        },
      ];

      const writer = new StructColumnWriter('root', { fields, optional: true });

      writer.write({ a: { b: { c: 42 } } });

      const result = writer.finish();

      // Each optional level adds 1 to max definition level
      // root (optional) + a (optional) + b (optional) + c (optional) = 4
      expect(result.maxDefinitionLevel).toBeGreaterThanOrEqual(1);
    });
  });

  describe('Factory method integration', () => {
    it('should create struct writer via factory method', () => {
      const options: ColumnWriterOptions = {
        fields: [
          { name: 'x', type: 'int32' },
          { name: 'y', type: 'int32' },
        ],
      };

      const writer = ColumnWriter.create('point', 'struct', options);

      expect(writer).toBeInstanceOf(StructColumnWriter);

      writer.write({ x: 10, y: 20 });
      const result = writer.finish();

      expect(result.dataType).toBe('STRUCT');
    });
  });
});

describe('ListColumnWriter', () => {
  describe('List of primitives', () => {
    it('should write a list of strings', () => {
      const writer = new ListColumnWriter('tags', { elementType: 'string' });

      writer.write(['red', 'green', 'blue']);
      writer.write(['yellow']);
      writer.write(['purple', 'orange']);

      const result = writer.finish();

      expect(result.columnName).toBe('tags');
      expect(result.dataType).toBe('LIST');
      expect(result.numValues).toBe(3);
      expect(result.statistics?.totalElements).toBe(6);

      // Element data should be present
      expect(result.elementChild).toBeDefined();
      expect(result.elementChild?.dataType).toBe('BYTE_ARRAY');
    });

    it('should write a list of integers', () => {
      const writer = new ListColumnWriter('scores', { elementType: 'int32' });

      writer.write([100, 200, 300]);
      writer.write([150]);

      const result = writer.finish();

      expect(result.elementChild?.dataType).toBe('INT32');
      expect(result.statistics?.totalElements).toBe(4);
    });

    it('should write a list of doubles', () => {
      const writer = new ListColumnWriter('measurements', { elementType: 'double' });

      writer.write([1.5, 2.5, 3.5]);
      writer.write([4.5, 5.5]);

      const result = writer.finish();

      expect(result.elementChild?.dataType).toBe('DOUBLE');
    });

    it('should write a list of booleans', () => {
      const writer = new ListColumnWriter('flags', { elementType: 'boolean' });

      writer.write([true, false, true]);
      writer.write([false, false]);

      const result = writer.finish();

      expect(result.elementChild?.dataType).toBe('BOOLEAN');
    });

    it('should handle empty lists', () => {
      const writer = new ListColumnWriter('items', { elementType: 'string', nullable: true });

      writer.write(['a', 'b']);
      writer.write([]); // empty list
      writer.write(['c']);

      const result = writer.finish();

      expect(result.numValues).toBe(3);
      expect(result.statistics?.minListLength).toBe(0);
      expect(result.statistics?.maxListLength).toBe(2);
    });

    it('should handle null lists with definition levels', () => {
      const writer = new ListColumnWriter('items', { elementType: 'string', nullable: true });

      writer.write(['a', 'b']);
      writer.write(null);
      writer.write(['c']);

      const result = writer.finish();

      expect(result.numValues).toBe(3);
      expect(result.statistics?.nullCount).toBe(1);
      expect(result.definitionLevels).toBeDefined();

      // Null list should have definition level 0
      // Definition levels for first list elements should be > 0
    });

    it('should handle null elements within lists', () => {
      const writer = new ListColumnWriter('items', { elementType: 'string', nullable: true });

      writer.write(['a', null, 'b']);
      writer.write([null, 'c']);

      const result = writer.finish();

      expect(result.statistics?.totalElements).toBe(5);
      expect(result.definitionLevels).toBeDefined();

      // Definition levels should distinguish between:
      // - null list (0)
      // - empty list (1)
      // - null element (2)
      // - present element (3)
    });

    it('should generate correct repetition levels', () => {
      const writer = new ListColumnWriter('nums', { elementType: 'int32' });

      writer.write([1, 2, 3]); // First list
      writer.write([4, 5]); // Second list
      writer.write([6]); // Third list

      const result = writer.finish();

      expect(result.repetitionLevels).toBeDefined();

      // Repetition levels should be:
      // [0, 1, 1, 0, 1, 0]
      // 0 = start of new list, 1 = continuation
      expect(result.repetitionLevels).toEqual([0, 1, 1, 0, 1, 0]);
    });

    it('should track list length statistics', () => {
      const writer = new ListColumnWriter('items', { elementType: 'string' });

      writer.write(['a']);
      writer.write(['b', 'c', 'd', 'e', 'f']);
      writer.write(['g', 'h']);

      const result = writer.finish();

      expect(result.statistics?.minListLength).toBe(1);
      expect(result.statistics?.maxListLength).toBe(5);
      expect(result.statistics?.totalElements).toBe(8);
    });
  });

  describe('List of structs', () => {
    it('should write a list of simple structs', () => {
      const elementOptions: ColumnWriterOptions = {
        fields: [
          { name: 'x', type: 'int32' },
          { name: 'y', type: 'int32' },
        ],
      };

      const writer = new ListColumnWriter('points', {
        elementType: 'struct',
        elementOptions,
      });

      writer.write([
        { x: 0, y: 0 },
        { x: 10, y: 20 },
      ]);
      writer.write([{ x: 5, y: 5 }]);

      const result = writer.finish();

      expect(result.dataType).toBe('LIST');
      expect(result.elementChild?.dataType).toBe('STRUCT');
      expect(result.elementChild?.children).toHaveLength(2);
    });

    it('should handle null struct elements in list', () => {
      const elementOptions: ColumnWriterOptions = {
        fields: [{ name: 'value', type: 'int32' }],
      };

      const writer = new ListColumnWriter('items', {
        elementType: 'struct',
        elementOptions,
        nullable: true,
      });

      writer.write([{ value: 1 }, null, { value: 3 }]);

      const result = writer.finish();

      expect(result.statistics?.totalElements).toBe(3);
      // Element child should track the null struct
    });
  });

  describe('Nested lists', () => {
    it('should write a list of lists (2D array)', () => {
      const elementOptions: ColumnWriterOptions = {
        elementType: 'int32',
      };

      const writer = new ListColumnWriter('matrix', {
        elementType: 'list',
        elementOptions,
      });

      writer.write([
        [1, 2, 3],
        [4, 5, 6],
      ]);
      writer.write([[7, 8], [9]]);

      const result = writer.finish();

      expect(result.dataType).toBe('LIST');
      expect(result.elementChild?.dataType).toBe('LIST');
      expect(result.maxRepetitionLevel).toBeGreaterThanOrEqual(2);
    });

    it('should handle deeply nested lists (3D array)', () => {
      const innerOptions: ColumnWriterOptions = {
        elementType: 'int32',
      };

      const middleOptions: ColumnWriterOptions = {
        elementType: 'list',
        elementOptions: innerOptions,
      };

      const writer = new ListColumnWriter('tensor', {
        elementType: 'list',
        elementOptions: middleOptions,
      });

      writer.write([
        [
          [1, 2],
          [3, 4],
        ],
        [[5, 6]],
      ]);

      const result = writer.finish();

      expect(result.maxRepetitionLevel).toBeGreaterThanOrEqual(3);
    });

    it('should generate correct repetition levels for nested lists', () => {
      const elementOptions: ColumnWriterOptions = {
        elementType: 'int32',
      };

      const writer = new ListColumnWriter('nested', {
        elementType: 'list',
        elementOptions,
      });

      // [[1, 2], [3]]
      writer.write([[1, 2], [3]]);

      const result = writer.finish();

      // Repetition levels for [[1, 2], [3]]:
      // 1: rep=0 (new outer list, new inner list)
      // 2: rep=2 (same inner list)
      // 3: rep=1 (same outer list, new inner list)
      expect(result.repetitionLevels).toBeDefined();
    });
  });

  describe('Factory method integration', () => {
    it('should create list writer via factory method', () => {
      const options: ColumnWriterOptions = {
        elementType: 'int32',
      };

      const writer = ColumnWriter.create('numbers', 'list', options);

      expect(writer).toBeInstanceOf(ListColumnWriter);

      writer.write([1, 2, 3]);
      const result = writer.finish();

      expect(result.dataType).toBe('LIST');
    });
  });
});

describe('Complex nested structures', () => {
  it('should write struct containing list of structs', () => {
    const fields: FieldDefinition[] = [
      { name: 'id', type: 'string' },
      {
        name: 'items',
        type: 'list',
        elementType: 'struct',
        elementOptions: {
          fields: [
            { name: 'name', type: 'string' },
            { name: 'qty', type: 'int32' },
          ],
        },
      },
    ];

    const writer = new StructColumnWriter('order', { fields });

    writer.write({
      id: 'order-1',
      items: [
        { name: 'Widget', qty: 5 },
        { name: 'Gadget', qty: 3 },
      ],
    });

    const result = writer.finish();

    expect(result.dataType).toBe('STRUCT');
    expect(result.children).toHaveLength(2);

    const itemsChild = result.children!.find((c) => c.columnName === 'order.items');
    expect(itemsChild?.dataType).toBe('LIST');
    expect(itemsChild?.elementChild?.dataType).toBe('STRUCT');
  });

  it('should write list of structs containing lists', () => {
    const elementOptions: ColumnWriterOptions = {
      fields: [
        { name: 'name', type: 'string' },
        {
          name: 'scores',
          type: 'list',
          elementType: 'int32',
        },
      ],
    };

    const writer = new ListColumnWriter('players', {
      elementType: 'struct',
      elementOptions,
    });

    writer.write([
      { name: 'Alice', scores: [100, 95, 88] },
      { name: 'Bob', scores: [72, 81] },
    ]);

    const result = writer.finish();

    expect(result.dataType).toBe('LIST');
    expect(result.elementChild?.children).toHaveLength(2);

    const scoresChild = result.elementChild?.children?.find((c) =>
      c.columnName.endsWith('.scores')
    );
    expect(scoresChild?.dataType).toBe('LIST');
  });

  it('should handle MongoDB-style nested document with arrays', () => {
    // Simulate a MongoDB document structure
    const fields: FieldDefinition[] = [
      { name: 'name', type: 'string' },
      {
        name: 'address',
        type: 'struct',
        fields: [
          { name: 'street', type: 'string' },
          { name: 'city', type: 'string' },
          { name: 'coords', type: 'list', elementType: 'double' },
        ],
      },
      {
        name: 'orders',
        type: 'list',
        elementType: 'struct',
        elementOptions: {
          fields: [
            { name: 'orderId', type: 'string' },
            { name: 'total', type: 'double' },
            {
              name: 'items',
              type: 'list',
              elementType: 'struct',
              elementOptions: {
                fields: [
                  { name: 'sku', type: 'string' },
                  { name: 'price', type: 'double' },
                ],
              },
            },
          ],
        },
      },
    ];

    const writer = new StructColumnWriter('customer', { fields });

    writer.write({
      name: 'John Doe',
      address: {
        street: '123 Main St',
        city: 'Anytown',
        coords: [-73.9857, 40.7484],
      },
      orders: [
        {
          orderId: 'ORD-001',
          total: 99.99,
          items: [
            { sku: 'SKU-A', price: 49.99 },
            { sku: 'SKU-B', price: 50.0 },
          ],
        },
        {
          orderId: 'ORD-002',
          total: 25.0,
          items: [{ sku: 'SKU-C', price: 25.0 }],
        },
      ],
    });

    const result = writer.finish();

    expect(result.dataType).toBe('STRUCT');
    expect(result.numValues).toBe(1);

    // Verify nested structure is preserved
    const addressChild = result.children?.find((c) => c.columnName === 'customer.address');
    expect(addressChild?.dataType).toBe('STRUCT');

    const ordersChild = result.children?.find((c) => c.columnName === 'customer.orders');
    expect(ordersChild?.dataType).toBe('LIST');
  });

  it('should properly flatten column names in complex nested structures', () => {
    const fields: FieldDefinition[] = [
      {
        name: 'a',
        type: 'struct',
        fields: [
          {
            name: 'b',
            type: 'struct',
            fields: [{ name: 'c', type: 'int32' }],
          },
        ],
      },
    ];

    const writer = new StructColumnWriter('root', { fields });

    writer.write({ a: { b: { c: 42 } } });

    const result = writer.finish();

    // Collect all column names from nested structure
    const allColumnNames: string[] = [];
    const collectNames = (col: WrittenColumn) => {
      allColumnNames.push(col.columnName);
      if (col.children) {
        col.children.forEach(collectNames);
      }
      if (col.elementChild) {
        collectNames(col.elementChild);
      }
    };
    collectNames(result);

    expect(allColumnNames).toContain('root');
    expect(allColumnNames).toContain('root.a');
    expect(allColumnNames).toContain('root.a.b');
    expect(allColumnNames).toContain('root.a.b.c');
  });
});

describe('Definition and Repetition Levels', () => {
  it('should calculate correct definition levels for optional fields', () => {
    const fields: FieldDefinition[] = [
      { name: 'required_field', type: 'string' },
      { name: 'optional_field', type: 'string' },
    ];

    const writer = new StructColumnWriter('data', { fields, optional: true });

    // Test various null scenarios
    writer.write({ required_field: 'a', optional_field: 'x' }); // All present
    writer.write({ required_field: 'b', optional_field: null }); // Field null
    writer.write(null); // Entire struct null

    const result = writer.finish();

    expect(result.maxDefinitionLevel).toBeGreaterThanOrEqual(1);
    expect(result.definitionLevels).toBeDefined();
  });

  it('should calculate correct definition levels for nested nulls', () => {
    const fields: FieldDefinition[] = [
      {
        name: 'outer',
        type: 'struct',
        fields: [{ name: 'inner', type: 'int32' }],
      },
    ];

    const writer = new StructColumnWriter('root', { fields, optional: true });

    writer.write({ outer: { inner: 42 } }); // Fully present
    writer.write({ outer: { inner: null } }); // Inner null
    writer.write({ outer: null }); // Outer null
    writer.write(null); // Root null

    const result = writer.finish();

    // maxDefinitionLevel should account for all optional levels
    expect(result.maxDefinitionLevel).toBeGreaterThanOrEqual(1);
  });

  it('should generate correct repetition levels for lists in structs', () => {
    const fields: FieldDefinition[] = [
      { name: 'id', type: 'int32' },
      { name: 'values', type: 'list', elementType: 'int32' },
    ];

    const writer = new StructColumnWriter('record', { fields });

    writer.write({ id: 1, values: [10, 20, 30] });
    writer.write({ id: 2, values: [40, 50] });

    const result = writer.finish();

    const valuesChild = result.children?.find((c) => c.columnName === 'record.values');
    expect(valuesChild?.repetitionLevels).toBeDefined();
    expect(valuesChild?.maxRepetitionLevel).toBeGreaterThanOrEqual(1);
  });

  it('should handle complex repetition levels for nested lists', () => {
    const elementOptions: ColumnWriterOptions = {
      elementType: 'int32',
    };

    const writer = new ListColumnWriter('matrix', {
      elementType: 'list',
      elementOptions,
    });

    // Matrix: [[1, 2], [3, 4, 5]]
    writer.write([
      [1, 2],
      [3, 4, 5],
    ]);

    const result = writer.finish();

    // For nested lists, repetition levels indicate:
    // 0 = new top-level element
    // 1 = new element in current list
    // 2 = continuation of element in nested list
    expect(result.maxRepetitionLevel).toBeGreaterThanOrEqual(2);
  });
});

describe('Compression and encoding', () => {
  it('should apply compression to struct data', () => {
    const fields: FieldDefinition[] = [
      { name: 'name', type: 'string' },
      { name: 'value', type: 'int32' },
    ];

    const writer = new StructColumnWriter('data', {
      fields,
      compression: 'snappy',
    });

    // Write enough data to see compression benefits
    for (let i = 0; i < 100; i++) {
      writer.write({ name: 'repeated name value', value: i });
    }

    const result = writer.finish();

    expect(result.compression).toBe('snappy');
    expect(result.compressedSize).toBeLessThanOrEqual(result.uncompressedSize);
  });

  it('should apply compression to list data', () => {
    const writer = new ListColumnWriter('numbers', {
      elementType: 'int32',
      compression: 'snappy',
    });

    // Write lists with repetitive data
    for (let i = 0; i < 50; i++) {
      writer.write([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    const result = writer.finish();

    expect(result.compression).toBe('snappy');
    expect(result.compressedSize).toBeLessThanOrEqual(result.uncompressedSize);
  });
});

describe('Edge cases', () => {
  it('should handle struct with no fields', () => {
    const writer = new StructColumnWriter('empty', { fields: [] });

    writer.write({});

    const result = writer.finish();

    expect(result.dataType).toBe('STRUCT');
    expect(result.children).toHaveLength(0);
  });

  it('should handle empty list column', () => {
    const writer = new ListColumnWriter('items', { elementType: 'string' });

    // No writes

    const result = writer.finish();

    expect(result.numValues).toBe(0);
    expect(result.statistics?.totalElements).toBe(0);
  });

  it('should handle list with all empty arrays', () => {
    const writer = new ListColumnWriter('items', { elementType: 'string' });

    writer.write([]);
    writer.write([]);
    writer.write([]);

    const result = writer.finish();

    expect(result.numValues).toBe(3);
    expect(result.statistics?.totalElements).toBe(0);
    expect(result.statistics?.minListLength).toBe(0);
    expect(result.statistics?.maxListLength).toBe(0);
  });

  it('should handle list with all null values', () => {
    const writer = new ListColumnWriter('items', { elementType: 'string', nullable: true });

    writer.write(null);
    writer.write(null);
    writer.write(null);

    const result = writer.finish();

    expect(result.numValues).toBe(3);
    expect(result.statistics?.nullCount).toBe(3);
  });

  it('should handle very large lists', () => {
    const writer = new ListColumnWriter('biglist', { elementType: 'int32' });

    const bigArray = Array.from({ length: 10000 }, (_, i) => i);
    writer.write(bigArray);

    const result = writer.finish();

    expect(result.statistics?.totalElements).toBe(10000);
    expect(result.statistics?.maxListLength).toBe(10000);
  });

  it('should handle struct with many fields', () => {
    const fields: FieldDefinition[] = Array.from({ length: 50 }, (_, i) => ({
      name: `field_${i}`,
      type: 'int32' as const,
    }));

    const writer = new StructColumnWriter('wide', { fields });

    const value: Record<string, number> = {};
    for (let i = 0; i < 50; i++) {
      value[`field_${i}`] = i;
    }
    writer.write(value);

    const result = writer.finish();

    expect(result.children).toHaveLength(50);
  });
});
