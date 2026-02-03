/**
 * Test Fixtures
 *
 * Reusable test data for consistent testing across the codebase.
 * Provides static fixtures for common test scenarios.
 */

import { ObjectId } from '../../src/types.js';
import type {
  UserDocument,
  OrderDocument,
  ProductDocument,
  AddressDocument,
  DeduplicationDocument,
} from './factories.js';

// ============================================================================
// ObjectId Fixtures
// ============================================================================

/**
 * Well-known ObjectIds for testing.
 */
export const OBJECT_IDS = {
  /** Standard test ObjectId */
  TEST_1: '507f1f77bcf86cd799439011',
  /** Another standard test ObjectId */
  TEST_2: '507f191e810c19729de860ea',
  /** ObjectId with timestamp at Unix epoch */
  EPOCH: '000000000000000000000000',
  /** ObjectId with max value */
  MAX: 'ffffffffffffffffffffffff',
  /** ObjectId with sequential bytes */
  SEQUENTIAL: '0123456789abcdef01234567',
  /** Invalid ObjectId (too short) */
  INVALID_SHORT: '507f1f77bcf86cd79943901',
  /** Invalid ObjectId (too long) */
  INVALID_LONG: '507f1f77bcf86cd7994390110',
  /** Invalid ObjectId (bad characters) */
  INVALID_CHARS: '507f1f77bcf86cd79943901g',
} as const;

/**
 * Create ObjectId instances from fixtures.
 */
export function getObjectIdInstance(key: keyof typeof OBJECT_IDS): ObjectId {
  const hex = OBJECT_IDS[key];
  if (!ObjectId.isValid(hex)) {
    throw new Error(`Invalid ObjectId fixture: ${key}`);
  }
  return new ObjectId(hex);
}

// ============================================================================
// User Fixtures
// ============================================================================

/**
 * Standard user fixtures for testing.
 */
export const USERS: Record<string, UserDocument> = {
  alice: {
    _id: 'user-alice',
    name: 'Alice Smith',
    email: 'alice@example.com',
    age: 30,
    status: 'active',
    tags: ['admin', 'verified'],
    profile: {
      firstName: 'Alice',
      lastName: 'Smith',
      bio: 'Software engineer with 10 years of experience.',
    },
    settings: {
      theme: 'dark',
      notifications: true,
      language: 'en',
    },
    address: {
      street: '123 Main St',
      city: 'New York',
      state: 'NY',
      country: 'USA',
      zip: '10001',
    },
    createdAt: new Date('2024-01-01T00:00:00Z'),
    updatedAt: new Date('2024-06-01T00:00:00Z'),
  },
  bob: {
    _id: 'user-bob',
    name: 'Bob Johnson',
    email: 'bob@example.com',
    age: 25,
    status: 'active',
    tags: ['user'],
    profile: {
      firstName: 'Bob',
      lastName: 'Johnson',
    },
    settings: {
      theme: 'light',
      notifications: false,
    },
    createdAt: new Date('2024-02-15T00:00:00Z'),
    updatedAt: new Date('2024-02-15T00:00:00Z'),
  },
  charlie: {
    _id: 'user-charlie',
    name: 'Charlie Brown',
    email: 'charlie@example.com',
    age: 35,
    status: 'inactive',
    tags: ['user', 'premium'],
    profile: {
      firstName: 'Charlie',
      lastName: 'Brown',
      bio: 'Product manager and tech enthusiast.',
    },
    createdAt: new Date('2023-06-01T00:00:00Z'),
    updatedAt: new Date('2024-01-01T00:00:00Z'),
  },
  diana: {
    _id: 'user-diana',
    name: 'Diana Prince',
    email: 'diana@example.com',
    age: 28,
    status: 'pending',
    tags: ['new'],
    createdAt: new Date('2024-06-01T00:00:00Z'),
    updatedAt: new Date('2024-06-01T00:00:00Z'),
  },
};

/**
 * Get all user fixtures as an array.
 */
export function getAllUsers(): UserDocument[] {
  return Object.values(USERS);
}

/**
 * Get users matching a filter.
 */
export function getUsersMatching(predicate: (user: UserDocument) => boolean): UserDocument[] {
  return getAllUsers().filter(predicate);
}

// ============================================================================
// Product Fixtures
// ============================================================================

/**
 * Standard product fixtures for testing.
 */
export const PRODUCTS: Record<string, ProductDocument> = {
  laptop: {
    _id: 'product-laptop',
    name: 'Professional Laptop',
    description: 'High-performance laptop for professionals',
    price: 1299.99,
    category: 'electronics',
    tags: ['laptop', 'computer', 'professional'],
    inventory: 50,
    metadata: {
      brand: 'TechCorp',
      model: 'Pro 15',
      specs: {
        cpu: 'Intel i7',
        ram: '16GB',
        storage: '512GB SSD',
      },
    },
    createdAt: new Date('2024-01-01T00:00:00Z'),
    updatedAt: new Date('2024-03-01T00:00:00Z'),
  },
  phone: {
    _id: 'product-phone',
    name: 'Smartphone X',
    description: 'Latest smartphone with advanced features',
    price: 999.99,
    category: 'electronics',
    tags: ['phone', 'mobile', 'smartphone'],
    inventory: 100,
    createdAt: new Date('2024-02-01T00:00:00Z'),
    updatedAt: new Date('2024-02-01T00:00:00Z'),
  },
  headphones: {
    _id: 'product-headphones',
    name: 'Wireless Headphones',
    description: 'Premium noise-canceling headphones',
    price: 299.99,
    category: 'electronics',
    tags: ['audio', 'headphones', 'wireless'],
    inventory: 75,
    createdAt: new Date('2024-03-01T00:00:00Z'),
    updatedAt: new Date('2024-03-01T00:00:00Z'),
  },
  book: {
    _id: 'product-book',
    name: 'Programming Guide',
    description: 'Comprehensive programming guide for beginners',
    price: 49.99,
    category: 'books',
    tags: ['book', 'programming', 'education'],
    inventory: 200,
    createdAt: new Date('2023-01-01T00:00:00Z'),
    updatedAt: new Date('2023-01-01T00:00:00Z'),
  },
  outOfStock: {
    _id: 'product-oos',
    name: 'Limited Edition Item',
    description: 'Rare collectible item',
    price: 599.99,
    category: 'collectibles',
    tags: ['rare', 'limited'],
    inventory: 0,
    createdAt: new Date('2023-06-01T00:00:00Z'),
    updatedAt: new Date('2024-01-01T00:00:00Z'),
  },
};

/**
 * Get all product fixtures as an array.
 */
export function getAllProducts(): ProductDocument[] {
  return Object.values(PRODUCTS);
}

// ============================================================================
// Order Fixtures
// ============================================================================

/**
 * Standard order fixtures for testing.
 */
export const ORDERS: Record<string, OrderDocument> = {
  pending: {
    _id: 'order-pending',
    userId: 'user-alice',
    items: [
      { productId: 'product-laptop', name: 'Professional Laptop', quantity: 1, price: 1299.99 },
      { productId: 'product-headphones', name: 'Wireless Headphones', quantity: 1, price: 299.99 },
    ],
    status: 'pending',
    total: 1599.98,
    shippingAddress: {
      street: '123 Main St',
      city: 'New York',
      state: 'NY',
      country: 'USA',
      zip: '10001',
    },
    createdAt: new Date('2024-06-01T10:00:00Z'),
    updatedAt: new Date('2024-06-01T10:00:00Z'),
  },
  processing: {
    _id: 'order-processing',
    userId: 'user-bob',
    items: [{ productId: 'product-phone', name: 'Smartphone X', quantity: 2, price: 999.99 }],
    status: 'processing',
    total: 1999.98,
    createdAt: new Date('2024-05-15T14:30:00Z'),
    updatedAt: new Date('2024-05-16T09:00:00Z'),
  },
  shipped: {
    _id: 'order-shipped',
    userId: 'user-charlie',
    items: [
      { productId: 'product-book', name: 'Programming Guide', quantity: 3, price: 49.99 },
    ],
    status: 'shipped',
    total: 149.97,
    createdAt: new Date('2024-04-01T08:00:00Z'),
    updatedAt: new Date('2024-04-05T16:00:00Z'),
  },
  delivered: {
    _id: 'order-delivered',
    userId: 'user-alice',
    items: [{ productId: 'product-headphones', name: 'Wireless Headphones', quantity: 1, price: 299.99 }],
    status: 'delivered',
    total: 299.99,
    createdAt: new Date('2024-01-15T11:00:00Z'),
    updatedAt: new Date('2024-01-20T15:30:00Z'),
  },
  cancelled: {
    _id: 'order-cancelled',
    userId: 'user-diana',
    items: [{ productId: 'product-laptop', name: 'Professional Laptop', quantity: 1, price: 1299.99 }],
    status: 'cancelled',
    total: 1299.99,
    createdAt: new Date('2024-03-01T09:00:00Z'),
    updatedAt: new Date('2024-03-02T10:00:00Z'),
  },
};

/**
 * Get all order fixtures as an array.
 */
export function getAllOrders(): OrderDocument[] {
  return Object.values(ORDERS);
}

// ============================================================================
// Address Fixtures
// ============================================================================

/**
 * Standard address fixtures for testing.
 */
export const ADDRESSES: Record<string, AddressDocument> = {
  newYork: {
    street: '350 5th Avenue',
    city: 'New York',
    state: 'NY',
    country: 'USA',
    zip: '10118',
    coordinates: {
      lat: 40.7484,
      lng: -73.9857,
    },
  },
  sanFrancisco: {
    street: '1 Market Street',
    city: 'San Francisco',
    state: 'CA',
    country: 'USA',
    zip: '94105',
    coordinates: {
      lat: 37.7749,
      lng: -122.4194,
    },
  },
  london: {
    street: '221B Baker Street',
    city: 'London',
    country: 'UK',
    zip: 'NW1 6XE',
    coordinates: {
      lat: 51.5238,
      lng: -0.1586,
    },
  },
  tokyo: {
    street: '1-1 Marunouchi',
    city: 'Tokyo',
    country: 'Japan',
    zip: '100-0005',
    coordinates: {
      lat: 35.6762,
      lng: 139.6503,
    },
  },
};

// ============================================================================
// Deduplication Fixtures
// ============================================================================

/**
 * Deduplication test scenarios.
 */
export const DEDUPLICATION_SCENARIOS: Record<string, DeduplicationDocument[]> = {
  /** Simple insert then update */
  simpleUpdate: [
    { _id: 'doc1', _seq: 1, _op: 'i', name: 'Original' },
    { _id: 'doc1', _seq: 2, _op: 'u', name: 'Updated' },
  ],
  /** Insert, multiple updates, then delete */
  fullLifecycle: [
    { _id: 'doc1', _seq: 1, _op: 'i', name: 'v1' },
    { _id: 'doc1', _seq: 2, _op: 'u', name: 'v2' },
    { _id: 'doc1', _seq: 3, _op: 'u', name: 'v3' },
    { _id: 'doc1', _seq: 4, _op: 'd' },
  ],
  /** Delete then resurrect */
  resurrect: [
    { _id: 'doc1', _seq: 1, _op: 'i', name: 'Original' },
    { _id: 'doc1', _seq: 2, _op: 'd' },
    { _id: 'doc1', _seq: 3, _op: 'i', name: 'Resurrected' },
  ],
  /** Multiple documents interleaved */
  interleaved: [
    { _id: 'a', _seq: 1, _op: 'i', name: 'A1' },
    { _id: 'b', _seq: 2, _op: 'i', name: 'B1' },
    { _id: 'a', _seq: 3, _op: 'u', name: 'A2' },
    { _id: 'c', _seq: 4, _op: 'i', name: 'C1' },
    { _id: 'b', _seq: 5, _op: 'u', name: 'B2' },
    { _id: 'a', _seq: 6, _op: 'u', name: 'A3' },
  ],
  /** Out of order sequence numbers */
  outOfOrder: [
    { _id: 'doc1', _seq: 5, _op: 'u', name: 'v5' },
    { _id: 'doc1', _seq: 2, _op: 'i', name: 'v2' },
    { _id: 'doc1', _seq: 3, _op: 'u', name: 'v3' },
  ],
  /** Documents from multiple files */
  multiFile: {
    file1: [
      { _id: 'doc1', _seq: 1, _op: 'i', name: 'File1 v1' },
      { _id: 'doc2', _seq: 2, _op: 'i', name: 'File1 Doc2' },
    ],
    file2: [
      { _id: 'doc1', _seq: 5, _op: 'u', name: 'File2 v2' },
      { _id: 'doc3', _seq: 6, _op: 'i', name: 'File2 Doc3' },
    ],
  } as unknown as DeduplicationDocument[],
};

// ============================================================================
// Filter Test Fixtures
// ============================================================================

/**
 * Common filter patterns for testing.
 */
export const FILTERS = {
  /** Match all documents */
  matchAll: {},
  /** Match by exact _id */
  byId: (id: string) => ({ _id: id }),
  /** Match by exact field value */
  byField: (field: string, value: unknown) => ({ [field]: value }),
  /** Match with comparison operators */
  comparison: {
    greaterThan: (field: string, value: number) => ({ [field]: { $gt: value } }),
    lessThan: (field: string, value: number) => ({ [field]: { $lt: value } }),
    between: (field: string, min: number, max: number) => ({
      [field]: { $gte: min, $lte: max },
    }),
  },
  /** Match with array operators */
  array: {
    in: (field: string, values: unknown[]) => ({ [field]: { $in: values } }),
    notIn: (field: string, values: unknown[]) => ({ [field]: { $nin: values } }),
  },
  /** Match with logical operators */
  logical: {
    and: (conditions: Record<string, unknown>[]) => ({ $and: conditions }),
    or: (conditions: Record<string, unknown>[]) => ({ $or: conditions }),
    nor: (conditions: Record<string, unknown>[]) => ({ $nor: conditions }),
  },
  /** Match with existence */
  exists: (field: string, exists: boolean = true) => ({ [field]: { $exists: exists } }),
  /** Match with regex */
  regex: (field: string, pattern: string) => ({ [field]: { $regex: pattern } }),
} as const;

// ============================================================================
// Update Operation Fixtures
// ============================================================================

/**
 * Common update operations for testing.
 */
export const UPDATES = {
  /** Set a field value */
  set: (fields: Record<string, unknown>) => ({ $set: fields }),
  /** Unset fields */
  unset: (fields: string[]) => ({
    $unset: Object.fromEntries(fields.map((f) => [f, ''])),
  }),
  /** Increment numeric field */
  inc: (field: string, amount: number = 1) => ({ $inc: { [field]: amount } }),
  /** Push to array */
  push: (field: string, value: unknown) => ({ $push: { [field]: value } }),
  /** Pull from array */
  pull: (field: string, value: unknown) => ({ $pull: { [field]: value } }),
  /** Add to set */
  addToSet: (field: string, value: unknown) => ({ $addToSet: { [field]: value } }),
  /** Complex update with multiple operators */
  complex: (
    setFields?: Record<string, unknown>,
    incFields?: Record<string, number>,
    unsetFields?: string[]
  ) => ({
    ...(setFields && { $set: setFields }),
    ...(incFields && { $inc: incFields }),
    ...(unsetFields && { $unset: Object.fromEntries(unsetFields.map((f) => [f, ''])) }),
  }),
} as const;

// ============================================================================
// Error Fixtures
// ============================================================================

/**
 * Common error messages for testing error handling.
 */
export const ERRORS = {
  notFound: 'Document not found',
  duplicateKey: 'Duplicate key error',
  invalidFilter: 'Invalid filter',
  invalidUpdate: 'Invalid update',
  invalidDocument: 'Invalid document',
  unauthorized: 'Unauthorized',
  forbidden: 'Forbidden',
  timeout: 'Request timeout',
  networkError: 'Network error',
  serverError: 'Internal server error',
  quotaExceeded: 'Quota exceeded',
  r2Error: 'R2 storage error',
} as const;

// ============================================================================
// Date Fixtures
// ============================================================================

/**
 * Common date fixtures for testing.
 */
export const DATES = {
  /** Unix epoch */
  epoch: new Date(0),
  /** Year 2000 */
  y2k: new Date('2000-01-01T00:00:00Z'),
  /** Start of 2024 */
  start2024: new Date('2024-01-01T00:00:00Z'),
  /** Mid 2024 */
  mid2024: new Date('2024-06-15T12:00:00Z'),
  /** End of 2024 */
  end2024: new Date('2024-12-31T23:59:59Z'),
  /** Far future */
  farFuture: new Date('2100-01-01T00:00:00Z'),
} as const;

// ============================================================================
// Parquet Fixtures
// ============================================================================

/**
 * Parquet magic bytes for testing file format validation.
 */
export const PARQUET_MAGIC = new Uint8Array([0x50, 0x41, 0x52, 0x31]); // 'PAR1'

/**
 * Create a minimal valid Parquet file header.
 */
export function createMinimalParquetHeader(): Uint8Array {
  // PAR1 magic at start and end with minimal metadata
  const header = new Uint8Array(8);
  header.set(PARQUET_MAGIC, 0);
  header.set(PARQUET_MAGIC, 4);
  return header;
}

// ============================================================================
// Large Dataset Generators
// ============================================================================

/**
 * Generate a large dataset of users for performance testing.
 */
export function generateLargeUserDataset(count: number): UserDocument[] {
  return Array.from({ length: count }, (_, i) => ({
    _id: `user-${i}`,
    name: `User ${i}`,
    email: `user${i}@example.com`,
    age: 18 + (i % 60),
    status: (['active', 'inactive', 'pending'] as const)[i % 3],
    tags: [`tag${i % 10}`],
    createdAt: new Date(Date.now() - i * 86400000),
    updatedAt: new Date(),
  }));
}

/**
 * Generate a large dataset of products for performance testing.
 */
export function generateLargeProductDataset(count: number): ProductDocument[] {
  const categories = ['electronics', 'books', 'clothing', 'home', 'sports'];
  return Array.from({ length: count }, (_, i) => ({
    _id: `product-${i}`,
    name: `Product ${i}`,
    description: `Description for product ${i}`,
    price: 9.99 + (i % 100) * 10,
    category: categories[i % categories.length],
    tags: [`tag${i % 20}`],
    inventory: i % 200,
    createdAt: new Date(Date.now() - i * 3600000),
    updatedAt: new Date(),
  }));
}
