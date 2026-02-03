/**
 * Aggregation Tests
 *
 * Tests for Collection.aggregate() method and aggregation pipeline stages:
 * - $match, $limit, $skip, $count, $sort
 * - $group with $sum, $avg, $min, $max, $first, $last, $push, $addToSet
 * - $unwind
 * - $lookup
 * - $project, $addFields, $unset
 * - Combined pipelines
 */

import { describe, it, expect } from 'vitest';
import { createTestCollection, createTestDatabase } from './test-helpers.js';

describe('Collection.aggregate()', () => {
  it('should return AggregationCursor', () => {
    const { collection } = createTestCollection();
    const cursor = collection.aggregate([{ $match: {} }]);
    expect(cursor).toBeDefined();
  });

  it('should support $match stage', async () => {
    const { collection } = createTestCollection();
    await collection.insertMany([
      { _id: '1', status: 'active' },
      { _id: '2', status: 'inactive' },
    ]);

    const results = await collection.aggregate([{ $match: { status: 'active' } }]).toArray();

    expect(results).toHaveLength(1);
  });

  it('should support $limit stage', async () => {
    const { collection } = createTestCollection();
    await collection.insertMany([
      { _id: '1' },
      { _id: '2' },
      { _id: '3' },
    ]);

    const results = await collection.aggregate([{ $limit: 2 }]).toArray();

    expect(results).toHaveLength(2);
  });

  it('should support $skip stage', async () => {
    const { collection } = createTestCollection();
    await collection.insertMany([
      { _id: '1', value: 1 },
      { _id: '2', value: 2 },
      { _id: '3', value: 3 },
    ]);

    const results = await collection.aggregate([{ $sort: { value: 1 } }, { $skip: 1 }]).toArray();

    expect(results).toHaveLength(2);
    expect((results[0] as { value: number }).value).toBe(2);
  });

  it('should support $count stage', async () => {
    const { collection } = createTestCollection();
    await collection.insertMany([
      { _id: '1' },
      { _id: '2' },
      { _id: '3' },
    ]);

    const results = await collection.aggregate([{ $count: 'total' }]).toArray();

    expect(results).toHaveLength(1);
    expect((results[0] as { total: number }).total).toBe(3);
  });

  describe('$group stage', () => {
    it('should group by field and count with $sum: 1', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A' },
        { _id: '2', category: 'B' },
        { _id: '3', category: 'A' },
        { _id: '4', category: 'A' },
      ]);

      const results = await collection.aggregate([
        { $group: { _id: '$category', count: { $sum: 1 } } },
        { $sort: { _id: 1 } },
      ]).toArray();

      expect(results).toHaveLength(2);
      expect(results[0]).toEqual({ _id: 'A', count: 3 });
      expect(results[1]).toEqual({ _id: 'B', count: 1 });
    });

    it('should group all documents with _id: null', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', value: 10 },
        { _id: '2', value: 20 },
        { _id: '3', value: 30 },
      ]);

      const results = await collection.aggregate([
        { $group: { _id: null, total: { $sum: '$value' } } },
      ]).toArray();

      expect(results).toHaveLength(1);
      expect(results[0]).toEqual({ _id: null, total: 60 });
    });

    it('should calculate $avg correctly', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A', score: 80 },
        { _id: '2', category: 'A', score: 90 },
        { _id: '3', category: 'B', score: 70 },
      ]);

      const results = await collection.aggregate([
        { $group: { _id: '$category', avgScore: { $avg: '$score' } } },
        { $sort: { _id: 1 } },
      ]).toArray();

      expect(results).toHaveLength(2);
      expect((results[0] as { avgScore: number }).avgScore).toBe(85);
      expect((results[1] as { avgScore: number }).avgScore).toBe(70);
    });

    it('should calculate $min correctly', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A', value: 30 },
        { _id: '2', category: 'A', value: 10 },
        { _id: '3', category: 'A', value: 20 },
      ]);

      const results = await collection.aggregate([
        { $group: { _id: '$category', minValue: { $min: '$value' } } },
      ]).toArray();

      expect(results).toHaveLength(1);
      expect((results[0] as { minValue: number }).minValue).toBe(10);
    });

    it('should calculate $max correctly', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A', value: 30 },
        { _id: '2', category: 'A', value: 10 },
        { _id: '3', category: 'A', value: 20 },
      ]);

      const results = await collection.aggregate([
        { $group: { _id: '$category', maxValue: { $max: '$value' } } },
      ]).toArray();

      expect(results).toHaveLength(1);
      expect((results[0] as { maxValue: number }).maxValue).toBe(30);
    });

    it('should get $first value', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A', name: 'First' },
        { _id: '2', category: 'A', name: 'Second' },
        { _id: '3', category: 'A', name: 'Third' },
      ]);

      const results = await collection.aggregate([
        { $sort: { _id: 1 } },
        { $group: { _id: '$category', firstName: { $first: '$name' } } },
      ]).toArray();

      expect(results).toHaveLength(1);
      expect((results[0] as { firstName: string }).firstName).toBe('First');
    });

    it('should get $last value', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A', name: 'First' },
        { _id: '2', category: 'A', name: 'Second' },
        { _id: '3', category: 'A', name: 'Third' },
      ]);

      const results = await collection.aggregate([
        { $sort: { _id: 1 } },
        { $group: { _id: '$category', lastName: { $last: '$name' } } },
      ]).toArray();

      expect(results).toHaveLength(1);
      expect((results[0] as { lastName: string }).lastName).toBe('Third');
    });

    it('should collect values with $push', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A', value: 1 },
        { _id: '2', category: 'A', value: 2 },
        { _id: '3', category: 'B', value: 3 },
      ]);

      const results = await collection.aggregate([
        { $sort: { _id: 1 } },
        { $group: { _id: '$category', values: { $push: '$value' } } },
        { $sort: { _id: 1 } },
      ]).toArray();

      expect(results).toHaveLength(2);
      expect((results[0] as { values: number[] }).values).toEqual([1, 2]);
      expect((results[1] as { values: number[] }).values).toEqual([3]);
    });

    it('should collect unique values with $addToSet', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A', tag: 'x' },
        { _id: '2', category: 'A', tag: 'y' },
        { _id: '3', category: 'A', tag: 'x' },
        { _id: '4', category: 'A', tag: 'z' },
      ]);

      const results = await collection.aggregate([
        { $group: { _id: '$category', tags: { $addToSet: '$tag' } } },
      ]).toArray();

      expect(results).toHaveLength(1);
      const tags = (results[0] as { tags: string[] }).tags;
      expect(tags).toHaveLength(3);
      expect(tags).toContain('x');
      expect(tags).toContain('y');
      expect(tags).toContain('z');
    });

    it('should support compound _id expressions', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', year: 2023, month: 1, sales: 100 },
        { _id: '2', year: 2023, month: 1, sales: 200 },
        { _id: '3', year: 2023, month: 2, sales: 150 },
      ]);

      const results = await collection.aggregate([
        { $group: { _id: { year: '$year', month: '$month' }, totalSales: { $sum: '$sales' } } },
        { $sort: { '_id.month': 1 } },
      ]).toArray();

      expect(results).toHaveLength(2);
      expect((results[0] as { _id: { year: number; month: number }; totalSales: number })._id).toEqual({ year: 2023, month: 1 });
      expect((results[0] as { totalSales: number }).totalSales).toBe(300);
      expect((results[1] as { _id: { year: number; month: number }; totalSales: number })._id).toEqual({ year: 2023, month: 2 });
      expect((results[1] as { totalSales: number }).totalSales).toBe(150);
    });

    it('should support nested field paths', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', user: { department: 'eng' }, score: 90 },
        { _id: '2', user: { department: 'eng' }, score: 80 },
        { _id: '3', user: { department: 'sales' }, score: 70 },
      ]);

      const results = await collection.aggregate([
        { $group: { _id: '$user.department', avgScore: { $avg: '$score' } } },
        { $sort: { _id: 1 } },
      ]).toArray();

      expect(results).toHaveLength(2);
      expect(results[0]).toEqual({ _id: 'eng', avgScore: 85 });
      expect(results[1]).toEqual({ _id: 'sales', avgScore: 70 });
    });
  });

  describe('$unwind stage', () => {
    it('should unwind an array field (string notation)', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'Alice', tags: ['a', 'b', 'c'] },
      ]);

      const results = await collection.aggregate([
        { $unwind: '$tags' },
      ]).toArray();

      expect(results).toHaveLength(3);
      expect(results.map((r) => (r as { tags: string }).tags)).toEqual(['a', 'b', 'c']);
      expect(results.every((r) => (r as { name: string }).name === 'Alice')).toBe(true);
    });

    it('should unwind an array field (object notation)', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'Bob', items: [1, 2] },
      ]);

      const results = await collection.aggregate([
        { $unwind: { path: '$items' } },
      ]).toArray();

      expect(results).toHaveLength(2);
      expect(results.map((r) => (r as { items: number }).items)).toEqual([1, 2]);
    });

    it('should drop documents with empty arrays by default', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'Alice', tags: ['a'] },
        { _id: '2', name: 'Bob', tags: [] },
        { _id: '3', name: 'Charlie', tags: ['b', 'c'] },
      ]);

      const results = await collection.aggregate([
        { $unwind: '$tags' },
        { $sort: { _id: 1 } },
      ]).toArray();

      expect(results).toHaveLength(3);
      expect(results.map((r) => (r as { name: string }).name)).toEqual(['Alice', 'Charlie', 'Charlie']);
    });

    it('should drop documents with null/missing arrays by default', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'Alice', tags: ['a'] },
        { _id: '2', name: 'Bob' },
        { _id: '3', name: 'Charlie', tags: null },
      ]);

      const results = await collection.aggregate([
        { $unwind: '$tags' },
      ]).toArray();

      expect(results).toHaveLength(1);
      expect((results[0] as { name: string }).name).toBe('Alice');
    });

    it('should preserve documents with empty arrays when preserveNullAndEmptyArrays is true', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'Alice', tags: ['a'] },
        { _id: '2', name: 'Bob', tags: [] },
      ]);

      const results = await collection.aggregate([
        { $unwind: { path: '$tags', preserveNullAndEmptyArrays: true } },
        { $sort: { _id: 1 } },
      ]).toArray();

      expect(results).toHaveLength(2);
      expect((results[0] as { tags: string }).tags).toBe('a');
      expect((results[1] as { tags: null }).tags).toBeNull();
    });

    it('should preserve documents with missing arrays when preserveNullAndEmptyArrays is true', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'Alice', tags: ['a'] },
        { _id: '2', name: 'Bob' },
      ]);

      const results = await collection.aggregate([
        { $unwind: { path: '$tags', preserveNullAndEmptyArrays: true } },
        { $sort: { _id: 1 } },
      ]).toArray();

      expect(results).toHaveLength(2);
      expect((results[0] as { tags: string }).tags).toBe('a');
      expect((results[1] as { tags: null }).tags).toBeNull();
    });

    it('should work with nested array paths', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', data: { values: [1, 2, 3] } },
      ]);

      const results = await collection.aggregate([
        { $unwind: '$data.values' },
      ]).toArray();

      expect(results).toHaveLength(3);
      expect(results.map((r) => (r as { data: { values: number } }).data.values)).toEqual([1, 2, 3]);
    });

    it('should unwind then group for counting occurrences', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', tags: ['js', 'ts'] },
        { _id: '2', tags: ['js', 'python'] },
        { _id: '3', tags: ['ts'] },
      ]);

      const results = await collection.aggregate([
        { $unwind: '$tags' },
        { $group: { _id: '$tags', count: { $sum: 1 } } },
        { $sort: { count: -1, _id: 1 } },
      ]).toArray();

      expect(results).toHaveLength(3);
      expect(results[0]).toEqual({ _id: 'js', count: 2 });
      expect(results[1]).toEqual({ _id: 'ts', count: 2 });
      expect(results[2]).toEqual({ _id: 'python', count: 1 });
    });
  });

  describe('$lookup stage', () => {
    it('should perform basic left outer join', async () => {
      const { db } = createTestDatabase('lookupdb');

      const orders = db.collection('orders');
      const products = db.collection('products');

      await products.insertMany([
        { _id: 'prod1', name: 'Widget', price: 10 },
        { _id: 'prod2', name: 'Gadget', price: 20 },
      ]);

      await orders.insertMany([
        { _id: 'order1', productId: 'prod1', quantity: 5 },
        { _id: 'order2', productId: 'prod2', quantity: 3 },
        { _id: 'order3', productId: 'prod3', quantity: 1 },
      ]);

      const results = await orders.aggregate([
        {
          $lookup: {
            from: 'products',
            localField: 'productId',
            foreignField: '_id',
            as: 'productDetails',
          },
        },
        { $sort: { _id: 1 } },
      ]).toArray();

      expect(results).toHaveLength(3);

      const order1 = results[0] as { productDetails: { name: string }[] };
      expect(order1.productDetails).toHaveLength(1);
      expect(order1.productDetails[0].name).toBe('Widget');

      const order2 = results[1] as { productDetails: { name: string }[] };
      expect(order2.productDetails).toHaveLength(1);
      expect(order2.productDetails[0].name).toBe('Gadget');

      const order3 = results[2] as { productDetails: unknown[] };
      expect(order3.productDetails).toHaveLength(0);
    });

    it('should handle multiple matches', async () => {
      const { db } = createTestDatabase('lookupdb2');

      const authors = db.collection('authors');
      const books = db.collection('books');

      await authors.insertMany([
        { _id: 'author1', name: 'Alice' },
      ]);

      await books.insertMany([
        { _id: 'book1', title: 'Book A', authorId: 'author1' },
        { _id: 'book2', title: 'Book B', authorId: 'author1' },
        { _id: 'book3', title: 'Book C', authorId: 'author1' },
      ]);

      const results = await authors.aggregate([
        {
          $lookup: {
            from: 'books',
            localField: '_id',
            foreignField: 'authorId',
            as: 'authoredBooks',
          },
        },
      ]).toArray();

      expect(results).toHaveLength(1);
      const author = results[0] as { authoredBooks: { title: string }[] };
      expect(author.authoredBooks).toHaveLength(3);
      expect(author.authoredBooks.map((b) => b.title).sort()).toEqual(['Book A', 'Book B', 'Book C']);
    });

    it('should work with $unwind after $lookup', async () => {
      const { db } = createTestDatabase('lookupdb3');

      const orders = db.collection('orders');
      const items = db.collection('items');

      await items.insertMany([
        { _id: 'item1', orderId: 'order1', product: 'A' },
        { _id: 'item2', orderId: 'order1', product: 'B' },
      ]);

      await orders.insertMany([
        { _id: 'order1', customer: 'John' },
      ]);

      const results = await orders.aggregate([
        {
          $lookup: {
            from: 'items',
            localField: '_id',
            foreignField: 'orderId',
            as: 'orderItems',
          },
        },
        { $unwind: '$orderItems' },
        { $sort: { 'orderItems.product': 1 } },
      ]).toArray();

      expect(results).toHaveLength(2);
      expect((results[0] as { orderItems: { product: string } }).orderItems.product).toBe('A');
      expect((results[1] as { orderItems: { product: string } }).orderItems.product).toBe('B');
    });

    it('should support nested field paths in localField and foreignField', async () => {
      const { db } = createTestDatabase('lookupdb4');

      const users = db.collection('users');
      const profiles = db.collection('profiles');

      await profiles.insertMany([
        { _id: 'p1', userId: 'u1', bio: 'Bio 1' },
      ]);

      await users.insertMany([
        { _id: 'u1', info: { profileId: 'p1' }, name: 'Alice' },
      ]);

      const results = await users.aggregate([
        {
          $lookup: {
            from: 'profiles',
            localField: 'info.profileId',
            foreignField: '_id',
            as: 'profile',
          },
        },
      ]).toArray();

      expect(results).toHaveLength(1);
      const user = results[0] as { profile: { bio: string }[] };
      expect(user.profile).toHaveLength(1);
      expect(user.profile[0].bio).toBe('Bio 1');
    });
  });

  describe('combined pipeline stages', () => {
    it('should support $match -> $group -> $sort', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', status: 'active', amount: 100 },
        { _id: '2', status: 'active', amount: 200 },
        { _id: '3', status: 'inactive', amount: 50 },
        { _id: '4', status: 'active', amount: 150 },
      ]);

      const results = await collection.aggregate([
        { $match: { status: 'active' } },
        { $group: { _id: null, total: { $sum: '$amount' }, count: { $sum: 1 } } },
      ]).toArray();

      expect(results).toHaveLength(1);
      expect((results[0] as { total: number }).total).toBe(450);
      expect((results[0] as { count: number }).count).toBe(3);
    });

    it('should support $project stage', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'Alice', email: 'alice@test.com', password: 'secret' },
      ]);

      const results = await collection.aggregate([
        { $project: { name: 1, email: 1 } },
      ]).toArray();

      expect(results).toHaveLength(1);
      expect(results[0]).toHaveProperty('name', 'Alice');
      expect(results[0]).toHaveProperty('email', 'alice@test.com');
      expect(results[0]).not.toHaveProperty('password');
    });

    it('should support $addFields stage', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', price: 100, quantity: 5 },
      ]);

      const results = await collection.aggregate([
        { $addFields: { total: '$price' } },
      ]).toArray();

      expect(results).toHaveLength(1);
      expect((results[0] as { total: number }).total).toBe(100);
    });

    it('should support $unset stage', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'Alice', temp: 'value', internal: 'data' },
      ]);

      const results = await collection.aggregate([
        { $unset: ['temp', 'internal'] },
      ]).toArray();

      expect(results).toHaveLength(1);
      expect(results[0]).toHaveProperty('name', 'Alice');
      expect(results[0]).not.toHaveProperty('temp');
      expect(results[0]).not.toHaveProperty('internal');
    });

    it('should support complex pipeline with multiple stages', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'electronics', price: 500, tags: ['new', 'sale'] },
        { _id: '2', category: 'electronics', price: 300, tags: ['sale'] },
        { _id: '3', category: 'books', price: 20, tags: ['new'] },
        { _id: '4', category: 'electronics', price: 800, tags: ['premium'] },
      ]);

      const results = await collection.aggregate([
        { $match: { category: 'electronics' } },
        { $unwind: '$tags' },
        { $group: { _id: '$tags', avgPrice: { $avg: '$price' }, count: { $sum: 1 } } },
        { $sort: { _id: 1 } },
      ]).toArray();

      expect(results).toHaveLength(3);
      expect(results.find((r) => (r as { _id: string })._id === 'sale')).toEqual({
        _id: 'sale',
        avgPrice: 400,
        count: 2,
      });
    });
  });

  describe('$facet stage', () => {
    it('should run multiple pipelines and return results in one document', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', status: 'active', category: 'A', value: 100 },
        { _id: '2', status: 'inactive', category: 'B', value: 200 },
        { _id: '3', status: 'active', category: 'A', value: 150 },
        { _id: '4', status: 'active', category: 'B', value: 300 },
      ]);

      const results = await collection.aggregate([
        {
          $facet: {
            activeCount: [
              { $match: { status: 'active' } },
              { $count: 'count' },
            ],
            byCategory: [
              { $group: { _id: '$category', total: { $sum: '$value' } } },
              { $sort: { _id: 1 } },
            ],
          },
        },
      ]).toArray();

      expect(results).toHaveLength(1);

      const result = results[0] as {
        activeCount: { count: number }[];
        byCategory: { _id: string; total: number }[];
      };

      expect(result.activeCount).toHaveLength(1);
      expect(result.activeCount[0].count).toBe(3);

      expect(result.byCategory).toHaveLength(2);
      expect(result.byCategory[0]).toEqual({ _id: 'A', total: 250 });
      expect(result.byCategory[1]).toEqual({ _id: 'B', total: 500 });
    });

    it('should handle empty facet pipelines', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', value: 10 },
        { _id: '2', value: 20 },
      ]);

      const results = await collection.aggregate([
        {
          $facet: {
            all: [],
            filtered: [{ $match: { value: { $gt: 15 } } }],
          },
        },
      ]).toArray();

      expect(results).toHaveLength(1);

      const result = results[0] as {
        all: { value: number }[];
        filtered: { value: number }[];
      };

      expect(result.all).toHaveLength(2);
      expect(result.filtered).toHaveLength(1);
      expect(result.filtered[0].value).toBe(20);
    });

    it('should process $facet after $match', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', status: 'active', department: 'eng', salary: 100 },
        { _id: '2', status: 'active', department: 'eng', salary: 120 },
        { _id: '3', status: 'inactive', department: 'sales', salary: 80 },
        { _id: '4', status: 'active', department: 'sales', salary: 90 },
      ]);

      const results = await collection.aggregate([
        { $match: { status: 'active' } },
        {
          $facet: {
            avgByDept: [
              { $group: { _id: '$department', avgSalary: { $avg: '$salary' } } },
              { $sort: { _id: 1 } },
            ],
            totalCount: [{ $count: 'n' }],
          },
        },
      ]).toArray();

      expect(results).toHaveLength(1);

      const result = results[0] as {
        avgByDept: { _id: string; avgSalary: number }[];
        totalCount: { n: number }[];
      };

      expect(result.avgByDept).toHaveLength(2);
      expect(result.avgByDept[0]).toEqual({ _id: 'eng', avgSalary: 110 });
      expect(result.avgByDept[1]).toEqual({ _id: 'sales', avgSalary: 90 });
      expect(result.totalCount[0].n).toBe(3);
    });

    it('should handle facets with $unwind', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', tags: ['js', 'ts'] },
        { _id: '2', tags: ['python'] },
        { _id: '3', tags: ['js', 'python'] },
      ]);

      const results = await collection.aggregate([
        {
          $facet: {
            tagCounts: [
              { $unwind: '$tags' },
              { $group: { _id: '$tags', count: { $sum: 1 } } },
              { $sort: { _id: 1 } },
            ],
            docCount: [{ $count: 'total' }],
          },
        },
      ]).toArray();

      expect(results).toHaveLength(1);

      const result = results[0] as {
        tagCounts: { _id: string; count: number }[];
        docCount: { total: number }[];
      };

      expect(result.tagCounts).toHaveLength(3);
      expect(result.tagCounts.find((t) => t._id === 'js')?.count).toBe(2);
      expect(result.tagCounts.find((t) => t._id === 'python')?.count).toBe(2);
      expect(result.tagCounts.find((t) => t._id === 'ts')?.count).toBe(1);
      expect(result.docCount[0].total).toBe(3);
    });
  });

  describe('$bucket stage', () => {
    it('should group documents into buckets based on boundaries', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', price: 5 },
        { _id: '2', price: 15 },
        { _id: '3', price: 25 },
        { _id: '4', price: 35 },
        { _id: '5', price: 45 },
      ]);

      const results = await collection.aggregate([
        {
          $bucket: {
            groupBy: '$price',
            boundaries: [0, 20, 40, 60],
          },
        },
      ]).toArray();

      expect(results).toHaveLength(3);
      expect(results[0]).toEqual({ _id: 0, count: 2 }); // 5, 15
      expect(results[1]).toEqual({ _id: 20, count: 2 }); // 25, 35
      expect(results[2]).toEqual({ _id: 40, count: 1 }); // 45
    });

    it('should use default bucket for values outside boundaries', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', age: 15 },
        { _id: '2', age: 25 },
        { _id: '3', age: 35 },
        { _id: '4', age: 75 },
        { _id: '5', age: 85 },
      ]);

      const results = await collection.aggregate([
        {
          $bucket: {
            groupBy: '$age',
            boundaries: [20, 40, 60],
            default: 'Other',
          },
        },
      ]).toArray();

      // boundaries [20, 40, 60] creates two buckets: [20, 40) and [40, 60)
      // - ages 25, 35 fall into bucket [20, 40)
      // - bucket [40, 60) is empty, so it's not included
      // - ages 15, 75, 85 fall outside boundaries -> 'Other'
      expect(results).toHaveLength(2);
      expect(results.find((r) => r._id === 20)).toEqual({ _id: 20, count: 2 }); // 25, 35
      expect(results.find((r) => r._id === 'Other')).toEqual({ _id: 'Other', count: 3 }); // 15, 75, 85
    });

    it('should support custom output accumulators', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', price: 50, name: 'Widget A' },
        { _id: '2', price: 75, name: 'Widget B' },
        { _id: '3', price: 150, name: 'Gadget A' },
        { _id: '4', price: 175, name: 'Gadget B' },
        { _id: '5', price: 250, name: 'Premium' },
      ]);

      const results = await collection.aggregate([
        {
          $bucket: {
            groupBy: '$price',
            boundaries: [0, 100, 200, 300],
            output: {
              count: { $sum: 1 },
              avgPrice: { $avg: '$price' },
              products: { $push: '$name' },
            },
          },
        },
      ]).toArray();

      expect(results).toHaveLength(3);

      const bucket0 = results.find((r) => r._id === 0) as {
        _id: number;
        count: number;
        avgPrice: number;
        products: string[];
      };
      expect(bucket0.count).toBe(2);
      expect(bucket0.avgPrice).toBe(62.5);
      expect(bucket0.products).toContain('Widget A');
      expect(bucket0.products).toContain('Widget B');

      const bucket100 = results.find((r) => r._id === 100) as {
        _id: number;
        count: number;
        avgPrice: number;
        products: string[];
      };
      expect(bucket100.count).toBe(2);
      expect(bucket100.avgPrice).toBe(162.5);

      const bucket200 = results.find((r) => r._id === 200) as {
        _id: number;
        count: number;
        avgPrice: number;
        products: string[];
      };
      expect(bucket200.count).toBe(1);
      expect(bucket200.products).toEqual(['Premium']);
    });

    it('should handle null and missing values with default bucket', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', score: 50 },
        { _id: '2', score: null },
        { _id: '3' }, // missing score
        { _id: '4', score: 75 },
      ]);

      const results = await collection.aggregate([
        {
          $bucket: {
            groupBy: '$score',
            boundaries: [0, 100],
            default: 'NoScore',
          },
        },
      ]).toArray();

      expect(results).toHaveLength(2);
      expect(results.find((r) => r._id === 0)).toEqual({ _id: 0, count: 2 }); // 50, 75
      expect(results.find((r) => r._id === 'NoScore')).toEqual({ _id: 'NoScore', count: 2 }); // null, missing
    });

    it('should work with $bucket after $match', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', status: 'active', amount: 50 },
        { _id: '2', status: 'active', amount: 150 },
        { _id: '3', status: 'inactive', amount: 75 },
        { _id: '4', status: 'active', amount: 250 },
      ]);

      const results = await collection.aggregate([
        { $match: { status: 'active' } },
        {
          $bucket: {
            groupBy: '$amount',
            boundaries: [0, 100, 200, 300],
            output: { total: { $sum: '$amount' } },
          },
        },
      ]).toArray();

      expect(results).toHaveLength(3);
      expect(results.find((r) => r._id === 0)).toEqual({ _id: 0, total: 50 });
      expect(results.find((r) => r._id === 100)).toEqual({ _id: 100, total: 150 });
      expect(results.find((r) => r._id === 200)).toEqual({ _id: 200, total: 250 });
    });

    it('should skip empty buckets', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', value: 5 },
        { _id: '2', value: 15 },
        { _id: '3', value: 95 },
      ]);

      const results = await collection.aggregate([
        {
          $bucket: {
            groupBy: '$value',
            boundaries: [0, 20, 40, 60, 80, 100],
          },
        },
      ]).toArray();

      // Only buckets with documents are returned
      expect(results).toHaveLength(2);
      expect(results[0]).toEqual({ _id: 0, count: 2 }); // 5, 15
      expect(results[1]).toEqual({ _id: 80, count: 1 }); // 95
    });

    it('should handle nested field paths in groupBy', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', data: { score: 25 } },
        { _id: '2', data: { score: 55 } },
        { _id: '3', data: { score: 85 } },
      ]);

      const results = await collection.aggregate([
        {
          $bucket: {
            groupBy: '$data.score',
            boundaries: [0, 50, 100],
          },
        },
      ]).toArray();

      expect(results).toHaveLength(2);
      expect(results[0]).toEqual({ _id: 0, count: 1 }); // 25
      expect(results[1]).toEqual({ _id: 50, count: 2 }); // 55, 85
    });
  });
});
