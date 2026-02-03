/**
 * Test Data Generator
 *
 * Utilities for generating large amounts of test data for load and scale testing.
 * Provides configurable document generators with realistic data patterns.
 */

// ============================================================================
// Types
// ============================================================================

export interface GeneratorOptions {
  /** Starting index for document IDs */
  startIndex?: number;
  /** ID prefix for generated documents */
  idPrefix?: string;
  /** Target document size in bytes (approximate) */
  targetSizeBytes?: number;
}

export interface BatchGeneratorOptions extends GeneratorOptions {
  /** Number of documents per batch */
  batchSize?: number;
  /** Delay between batches in ms (for backpressure simulation) */
  batchDelayMs?: number;
}

export interface UserDocument {
  _id: string;
  name: string;
  email: string;
  age: number;
  department: string;
  salary: number;
  active: boolean;
  tags: string[];
  createdAt: string;
  metadata: {
    source: string;
    batchId: number;
    region: string;
  };
}

export interface EventDocument {
  _id: string;
  eventType: string;
  userId: string;
  sessionId: string;
  timestamp: string;
  properties: Record<string, unknown>;
  context: {
    userAgent: string;
    ip: string;
    locale: string;
    timezone: string;
  };
}

export interface OrderDocument {
  _id: string;
  customerId: string;
  status: string;
  items: Array<{
    productId: string;
    name: string;
    quantity: number;
    price: number;
  }>;
  totals: {
    subtotal: number;
    tax: number;
    shipping: number;
    total: number;
  };
  shippingAddress: {
    street: string;
    city: string;
    state: string;
    zip: string;
    country: string;
  };
  createdAt: string;
  updatedAt: string;
}

export interface TimeSeriesDocument {
  _id: string;
  metricName: string;
  value: number;
  timestamp: string;
  tags: Record<string, string>;
  dimensions: {
    host: string;
    region: string;
    service: string;
  };
}

export interface NestedDocument {
  _id: string;
  level1: {
    field1: string;
    level2: {
      field2: number;
      level3: {
        field3: boolean;
        level4: {
          field4: string[];
          data: string;
        };
      };
    };
  };
  metadata: {
    version: number;
    createdAt: string;
  };
}

// ============================================================================
// Constants
// ============================================================================

const DEPARTMENTS = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance', 'Operations', 'Legal', 'Support'];
const REGIONS = ['us-east', 'us-west', 'eu-west', 'eu-central', 'ap-south', 'ap-east', 'ap-north', 'sa-east'];
const TAGS = ['senior', 'junior', 'remote', 'onsite', 'fulltime', 'contractor', 'manager', 'lead', 'intern'];
const EVENT_TYPES = ['page_view', 'click', 'scroll', 'form_submit', 'purchase', 'signup', 'login', 'logout', 'error'];
const ORDER_STATUSES = ['pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded'];
const METRIC_NAMES = ['cpu_usage', 'memory_usage', 'disk_io', 'network_in', 'network_out', 'request_count', 'error_rate', 'latency_p99'];
const CITIES = ['New York', 'San Francisco', 'London', 'Tokyo', 'Paris', 'Berlin', 'Sydney', 'Toronto', 'Singapore', 'Mumbai'];
const COUNTRIES = ['USA', 'USA', 'UK', 'Japan', 'France', 'Germany', 'Australia', 'Canada', 'Singapore', 'India'];

// ============================================================================
// Base Generator Class
// ============================================================================

/**
 * Abstract base class for document generators
 */
export abstract class DocumentGenerator<T> {
  protected index: number;
  protected readonly idPrefix: string;
  protected readonly targetSize: number;

  constructor(options: GeneratorOptions = {}) {
    this.index = options.startIndex ?? 0;
    this.idPrefix = options.idPrefix ?? 'doc';
    this.targetSize = options.targetSizeBytes ?? 0;
  }

  /**
   * Generate a single document
   */
  abstract generate(): T;

  /**
   * Generate multiple documents
   */
  generateBatch(count: number): T[] {
    const batch: T[] = [];
    for (let i = 0; i < count; i++) {
      batch.push(this.generate());
    }
    return batch;
  }

  /**
   * Create an async iterator for streaming document generation
   */
  async *stream(count: number, batchSize: number = 1000, delayMs: number = 0): AsyncGenerator<T[]> {
    let remaining = count;
    while (remaining > 0) {
      const size = Math.min(batchSize, remaining);
      yield this.generateBatch(size);
      remaining -= size;
      if (delayMs > 0 && remaining > 0) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }
  }

  /**
   * Get current index
   */
  getIndex(): number {
    return this.index;
  }

  /**
   * Reset index
   */
  resetIndex(startIndex: number = 0): void {
    this.index = startIndex;
  }

  /**
   * Add padding to reach target size
   */
  protected padToSize(doc: Record<string, unknown>, targetBytes: number): void {
    if (targetBytes <= 0) return;

    const currentSize = JSON.stringify(doc).length;
    const paddingNeeded = Math.max(0, targetBytes - currentSize - 20);

    if (paddingNeeded > 0) {
      (doc as Record<string, unknown>).padding = 'x'.repeat(paddingNeeded);
    }
  }
}

// ============================================================================
// User Document Generator
// ============================================================================

export class UserDocumentGenerator extends DocumentGenerator<UserDocument> {
  generate(): UserDocument {
    const idx = this.index++;
    const doc: UserDocument = {
      _id: `${this.idPrefix}-${idx.toString().padStart(8, '0')}`,
      name: `User ${idx}`,
      email: `user${idx}@example.com`,
      age: 22 + (idx % 43),
      department: DEPARTMENTS[idx % DEPARTMENTS.length]!,
      salary: 50000 + (idx % 100) * 1000,
      active: idx % 10 !== 0,
      tags: [TAGS[idx % TAGS.length]!, TAGS[(idx + 3) % TAGS.length]!],
      createdAt: new Date(Date.now() - idx * 60000).toISOString(),
      metadata: {
        source: 'load-test',
        batchId: Math.floor(idx / 1000),
        region: REGIONS[idx % REGIONS.length]!,
      },
    };

    if (this.targetSize > 0) {
      this.padToSize(doc as unknown as Record<string, unknown>, this.targetSize);
    }

    return doc;
  }
}

// ============================================================================
// Event Document Generator
// ============================================================================

export class EventDocumentGenerator extends DocumentGenerator<EventDocument> {
  generate(): EventDocument {
    const idx = this.index++;
    const eventType = EVENT_TYPES[idx % EVENT_TYPES.length]!;

    const doc: EventDocument = {
      _id: `${this.idPrefix}-${idx.toString().padStart(10, '0')}`,
      eventType,
      userId: `user-${(idx % 10000).toString().padStart(5, '0')}`,
      sessionId: `session-${(idx % 50000).toString().padStart(6, '0')}`,
      timestamp: new Date(Date.now() - idx * 1000).toISOString(),
      properties: {
        page: `/page-${idx % 100}`,
        referrer: idx % 5 === 0 ? 'https://google.com' : 'direct',
        duration: 100 + (idx % 10000),
        isFirstVisit: idx % 100 === 0,
      },
      context: {
        userAgent: `Mozilla/5.0 (variant ${idx % 50})`,
        ip: `192.168.${(idx % 256)}.${(idx % 128)}`,
        locale: ['en-US', 'en-GB', 'fr-FR', 'de-DE', 'ja-JP'][idx % 5]!,
        timezone: ['America/New_York', 'Europe/London', 'Asia/Tokyo'][idx % 3]!,
      },
    };

    if (this.targetSize > 0) {
      this.padToSize(doc as unknown as Record<string, unknown>, this.targetSize);
    }

    return doc;
  }
}

// ============================================================================
// Order Document Generator
// ============================================================================

export class OrderDocumentGenerator extends DocumentGenerator<OrderDocument> {
  generate(): OrderDocument {
    const idx = this.index++;
    const itemCount = 1 + (idx % 5);
    const items = Array.from({ length: itemCount }, (_, i) => ({
      productId: `prod-${((idx * 7 + i) % 1000).toString().padStart(4, '0')}`,
      name: `Product ${(idx + i) % 500}`,
      quantity: 1 + (i % 3),
      price: 9.99 + (i * 10) + (idx % 50),
    }));

    const subtotal = items.reduce((sum, item) => sum + item.price * item.quantity, 0);
    const tax = subtotal * 0.08;
    const shipping = idx % 10 === 0 ? 0 : 9.99;
    const cityIdx = idx % CITIES.length;

    const doc: OrderDocument = {
      _id: `${this.idPrefix}-${idx.toString().padStart(8, '0')}`,
      customerId: `cust-${(idx % 5000).toString().padStart(5, '0')}`,
      status: ORDER_STATUSES[idx % ORDER_STATUSES.length]!,
      items,
      totals: {
        subtotal: Math.round(subtotal * 100) / 100,
        tax: Math.round(tax * 100) / 100,
        shipping,
        total: Math.round((subtotal + tax + shipping) * 100) / 100,
      },
      shippingAddress: {
        street: `${100 + (idx % 999)} Main Street`,
        city: CITIES[cityIdx]!,
        state: cityIdx < 2 ? 'CA' : '',
        zip: `${10000 + (idx % 90000)}`,
        country: COUNTRIES[cityIdx]!,
      },
      createdAt: new Date(Date.now() - idx * 3600000).toISOString(),
      updatedAt: new Date(Date.now() - idx * 1800000).toISOString(),
    };

    if (this.targetSize > 0) {
      this.padToSize(doc as unknown as Record<string, unknown>, this.targetSize);
    }

    return doc;
  }
}

// ============================================================================
// Time Series Document Generator
// ============================================================================

export class TimeSeriesDocumentGenerator extends DocumentGenerator<TimeSeriesDocument> {
  private readonly baseTimestamp: number;

  constructor(options: GeneratorOptions = {}) {
    super(options);
    this.baseTimestamp = Date.now();
  }

  generate(): TimeSeriesDocument {
    const idx = this.index++;
    const metricName = METRIC_NAMES[idx % METRIC_NAMES.length]!;

    // Generate realistic values based on metric type
    let value: number;
    switch (metricName) {
      case 'cpu_usage':
      case 'memory_usage':
        value = 20 + Math.sin(idx / 100) * 30 + (idx % 50);
        break;
      case 'error_rate':
        value = 0.1 + (idx % 10) * 0.01;
        break;
      case 'latency_p99':
        value = 50 + (idx % 200) + Math.random() * 50;
        break;
      default:
        value = 1000 + (idx % 5000);
    }

    const doc: TimeSeriesDocument = {
      _id: `${this.idPrefix}-${idx.toString().padStart(12, '0')}`,
      metricName,
      value: Math.round(value * 100) / 100,
      timestamp: new Date(this.baseTimestamp - idx * 10000).toISOString(),
      tags: {
        env: ['prod', 'staging', 'dev'][idx % 3]!,
        version: `v${1 + (idx % 10)}.${idx % 5}.0`,
      },
      dimensions: {
        host: `host-${(idx % 100).toString().padStart(3, '0')}`,
        region: REGIONS[idx % REGIONS.length]!,
        service: ['api', 'web', 'worker', 'cache', 'db'][idx % 5]!,
      },
    };

    if (this.targetSize > 0) {
      this.padToSize(doc as unknown as Record<string, unknown>, this.targetSize);
    }

    return doc;
  }
}

// ============================================================================
// Nested Document Generator
// ============================================================================

export class NestedDocumentGenerator extends DocumentGenerator<NestedDocument> {
  generate(): NestedDocument {
    const idx = this.index++;

    const doc: NestedDocument = {
      _id: `${this.idPrefix}-${idx.toString().padStart(8, '0')}`,
      level1: {
        field1: `value-${idx}-level1`,
        level2: {
          field2: idx * 100,
          level3: {
            field3: idx % 2 === 0,
            level4: {
              field4: [`tag-${idx % 10}`, `tag-${(idx + 1) % 10}`],
              data: `nested-data-${idx}`,
            },
          },
        },
      },
      metadata: {
        version: 1 + (idx % 5),
        createdAt: new Date(Date.now() - idx * 1000).toISOString(),
      },
    };

    if (this.targetSize > 0) {
      this.padToSize(doc as unknown as Record<string, unknown>, this.targetSize);
    }

    return doc;
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Generate a batch of random documents using a generator
 */
export function generateDocuments<T>(
  generator: DocumentGenerator<T>,
  count: number
): T[] {
  return generator.generateBatch(count);
}

/**
 * Stream documents with backpressure control
 */
export async function* streamDocuments<T>(
  generator: DocumentGenerator<T>,
  count: number,
  batchSize: number = 1000,
  delayMs: number = 0
): AsyncGenerator<T[]> {
  yield* generator.stream(count, batchSize, delayMs);
}

/**
 * Create a simple test document with configurable size
 */
export function createTestDocument(index: number, sizeBytes?: number): Record<string, unknown> {
  const doc: Record<string, unknown> = {
    _id: `doc-${index.toString().padStart(8, '0')}`,
    name: `Test Document ${index}`,
    value: index,
    active: index % 2 === 0,
    score: Math.random() * 100,
    createdAt: new Date().toISOString(),
    tags: [`tag-${index % 10}`, `tag-${(index + 1) % 10}`],
  };

  if (sizeBytes && sizeBytes > 0) {
    const currentSize = JSON.stringify(doc).length;
    const paddingNeeded = Math.max(0, sizeBytes - currentSize - 20);
    if (paddingNeeded > 0) {
      doc.padding = 'x'.repeat(paddingNeeded);
    }
  }

  return doc;
}

/**
 * Generate an array of test documents
 */
export function createTestDocuments(
  count: number,
  options: GeneratorOptions = {}
): Record<string, unknown>[] {
  const docs: Record<string, unknown>[] = [];
  const startIndex = options.startIndex ?? 0;

  for (let i = 0; i < count; i++) {
    docs.push(createTestDocument(startIndex + i, options.targetSizeBytes));
  }

  return docs;
}

/**
 * Calculate approximate memory usage for documents
 */
export function estimateMemoryUsage(docs: unknown[]): number {
  if (docs.length === 0) return 0;

  // Sample a few documents to estimate average size
  const sampleSize = Math.min(10, docs.length);
  let totalSize = 0;

  for (let i = 0; i < sampleSize; i++) {
    totalSize += JSON.stringify(docs[i]).length;
  }

  const avgSize = totalSize / sampleSize;
  return Math.ceil(avgSize * docs.length);
}

/**
 * Format byte size for display
 */
export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const kb = bytes / 1024;
  if (kb < 1024) return `${kb.toFixed(2)} KB`;
  const mb = kb / 1024;
  if (mb < 1024) return `${mb.toFixed(2)} MB`;
  const gb = mb / 1024;
  return `${gb.toFixed(2)} GB`;
}

/**
 * Format duration for display
 */
export function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
  const minutes = Math.floor(ms / 60000);
  const seconds = ((ms % 60000) / 1000).toFixed(1);
  return `${minutes}m ${seconds}s`;
}

/**
 * Calculate throughput metrics
 */
export function calculateThroughput(
  operationCount: number,
  durationMs: number
): { opsPerSecond: number; msPerOp: number } {
  return {
    opsPerSecond: durationMs > 0 ? (operationCount / durationMs) * 1000 : 0,
    msPerOp: operationCount > 0 ? durationMs / operationCount : 0,
  };
}

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create a user document generator
 */
export function createUserGenerator(options?: GeneratorOptions): UserDocumentGenerator {
  return new UserDocumentGenerator(options);
}

/**
 * Create an event document generator
 */
export function createEventGenerator(options?: GeneratorOptions): EventDocumentGenerator {
  return new EventDocumentGenerator(options);
}

/**
 * Create an order document generator
 */
export function createOrderGenerator(options?: GeneratorOptions): OrderDocumentGenerator {
  return new OrderDocumentGenerator(options);
}

/**
 * Create a time series document generator
 */
export function createTimeSeriesGenerator(options?: GeneratorOptions): TimeSeriesDocumentGenerator {
  return new TimeSeriesDocumentGenerator(options);
}

/**
 * Create a nested document generator
 */
export function createNestedGenerator(options?: GeneratorOptions): NestedDocumentGenerator {
  return new NestedDocumentGenerator(options);
}
