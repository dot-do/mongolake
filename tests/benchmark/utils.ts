/**
 * Benchmark Utilities
 *
 * Common utilities for running performance benchmarks in MongoLake.
 * Provides statistics calculation, timing, and result formatting.
 */

// ============================================================================
// Types
// ============================================================================

export interface BenchmarkResult {
  name: string;
  iterations: number;
  samples: number[];
  p50: number;
  p95: number;
  p99: number;
  min: number;
  max: number;
  mean: number;
  stdDev: number;
  opsPerSec: number;
  totalOps?: number;
}

export interface BenchmarkOptions {
  /** Number of iterations to run (default: 20) */
  iterations?: number;
  /** Number of warmup iterations (default: 3) */
  warmup?: number;
  /** Operations per iteration for batch benchmarks */
  batchSize?: number;
}

// ============================================================================
// Statistics Functions
// ============================================================================

/**
 * Calculate percentile from sorted samples
 */
function percentile(sorted: number[], p: number): number {
  const index = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
}

/**
 * Calculate benchmark statistics from samples
 */
function calculateStats(
  name: string,
  samples: number[],
  batchSize: number = 1
): BenchmarkResult {
  const sorted = [...samples].sort((a, b) => a - b);
  const n = samples.length;
  const sum = samples.reduce((a, b) => a + b, 0);
  const mean = sum / n;

  // Standard deviation
  const variance = samples.reduce((acc, s) => acc + Math.pow(s - mean, 2), 0) / n;
  const stdDev = Math.sqrt(variance);

  // Calculate ops/sec based on batch size
  const totalOps = n * batchSize;
  const totalTimeMs = sum;
  const opsPerSec = (totalOps / totalTimeMs) * 1000;

  return {
    name,
    iterations: n,
    samples,
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    min: sorted[0],
    max: sorted[n - 1],
    mean,
    stdDev,
    opsPerSec,
    totalOps,
  };
}

// ============================================================================
// Benchmark Runner
// ============================================================================

/**
 * Run a benchmark function and collect timing statistics
 *
 * @param name - Benchmark name
 * @param fn - Function to benchmark
 * @param options - Benchmark options
 * @returns Benchmark results with statistics
 */
export async function runBenchmark(
  name: string,
  fn: () => Promise<void> | void,
  options: BenchmarkOptions = {}
): Promise<BenchmarkResult> {
  const { iterations = 20, warmup = 3, batchSize = 1 } = options;
  const samples: number[] = [];

  // Warmup iterations (not measured)
  for (let i = 0; i < warmup; i++) {
    await fn();
  }

  // Measured iterations
  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    await fn();
    samples.push(performance.now() - start);
  }

  return calculateStats(name, samples, batchSize);
}

/**
 * Run a synchronous benchmark function
 */
export function runBenchmarkSync(
  name: string,
  fn: () => void,
  options: BenchmarkOptions = {}
): BenchmarkResult {
  const { iterations = 20, warmup = 3, batchSize = 1 } = options;
  const samples: number[] = [];

  // Warmup
  for (let i = 0; i < warmup; i++) {
    fn();
  }

  // Measured
  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    fn();
    samples.push(performance.now() - start);
  }

  return calculateStats(name, samples, batchSize);
}

// ============================================================================
// Formatting Functions
// ============================================================================

/**
 * Format a single benchmark result for display
 */
export function formatResult(result: BenchmarkResult): string {
  return `
${result.name}:
  Iterations: ${result.iterations}
  p50 (median): ${result.p50.toFixed(3)}ms
  p95: ${result.p95.toFixed(3)}ms
  p99: ${result.p99.toFixed(3)}ms
  Mean: ${result.mean.toFixed(3)}ms
  Min: ${result.min.toFixed(3)}ms
  Max: ${result.max.toFixed(3)}ms
  Std Dev: ${result.stdDev.toFixed(3)}ms
  Ops/sec: ${result.opsPerSec.toFixed(2)}`;
}

/**
 * Print a table of benchmark results
 */
export function printSummaryTable(results: BenchmarkResult[]): void {
  console.log('\n' + '='.repeat(100));
  console.log('SUMMARY');
  console.log('='.repeat(100));
  console.log(
    '\n' +
      'Benchmark'.padEnd(50) +
      'p50 (ms)'.padStart(12) +
      'p95 (ms)'.padStart(12) +
      'ops/sec'.padStart(14)
  );
  console.log('-'.repeat(88));

  for (const r of results) {
    const name = r.name.length > 48 ? r.name.slice(0, 48) + '..' : r.name;
    console.log(
      `${name.padEnd(50)} ${r.p50.toFixed(3).padStart(12)} ${r.p95.toFixed(3).padStart(12)} ${r.opsPerSec.toFixed(2).padStart(14)}`
    );
  }

  console.log('\n' + '='.repeat(100));
}

/**
 * Print a section header
 */
export function printHeader(title: string): void {
  console.log('\n' + '='.repeat(70));
  console.log(title);
  console.log('='.repeat(70));
}

/**
 * Print a section divider
 */
export function printDivider(title: string): void {
  console.log('\n' + '-'.repeat(70));
  console.log(title);
  console.log('-'.repeat(70));
}

// ============================================================================
// Data Generation
// ============================================================================

/**
 * Generate a simple document for testing
 */
export function generateSimpleDoc(index: number): Record<string, unknown> {
  return {
    _id: `doc-${index}`,
    name: `User ${index}`,
    email: `user${index}@example.com`,
    age: 20 + (index % 50),
    active: index % 2 === 0,
    createdAt: new Date().toISOString(),
  };
}

/**
 * Generate a medium-sized document with nested data
 */
export function generateMediumDoc(index: number): Record<string, unknown> {
  return {
    _id: `doc-${index}`,
    name: `User ${index}`,
    email: `user${index}@example.com`,
    age: 20 + (index % 50),
    active: index % 2 === 0,
    profile: {
      bio: `This is the biography for user ${index}. It contains some text.`,
      avatar: `https://example.com/avatars/${index}.png`,
      location: {
        city: ['New York', 'San Francisco', 'London', 'Tokyo', 'Paris'][index % 5],
        country: ['USA', 'USA', 'UK', 'Japan', 'France'][index % 5],
        coordinates: {
          lat: 40.7128 + (index % 10) * 0.1,
          lng: -74.006 + (index % 10) * 0.1,
        },
      },
    },
    preferences: {
      theme: index % 2 === 0 ? 'dark' : 'light',
      notifications: {
        email: true,
        push: index % 3 === 0,
        sms: false,
      },
      language: ['en', 'es', 'fr', 'de', 'ja'][index % 5],
    },
    tags: [`tag${index % 10}`, `tag${(index + 1) % 10}`, `tag${(index + 2) % 10}`],
    metadata: {
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      version: 1,
    },
  };
}

/**
 * Generate a large document with arrays and nested data
 */
export function generateLargeDoc(index: number): Record<string, unknown> {
  const items = Array.from({ length: 20 }, (_, i) => ({
    id: `item-${index}-${i}`,
    name: `Item ${i}`,
    price: 9.99 + i,
    quantity: 1 + (i % 5),
    attributes: {
      color: ['red', 'blue', 'green', 'yellow', 'black'][i % 5],
      size: ['S', 'M', 'L', 'XL'][i % 4],
      weight: 0.5 + i * 0.1,
    },
  }));

  return {
    _id: `doc-${index}`,
    orderId: `ORD-${Date.now()}-${index}`,
    customer: {
      id: `CUST-${index}`,
      name: `Customer ${index}`,
      email: `customer${index}@example.com`,
      address: {
        street: `${100 + index} Main Street`,
        city: ['New York', 'San Francisco', 'London', 'Tokyo', 'Paris'][index % 5],
        state: 'CA',
        zip: `9${(10000 + index).toString().slice(1)}`,
        country: 'USA',
      },
    },
    items,
    totals: {
      subtotal: items.reduce((sum, item) => sum + item.price * item.quantity, 0),
      tax: 0.08 * items.reduce((sum, item) => sum + item.price * item.quantity, 0),
      shipping: 9.99,
      discount: index % 10 === 0 ? 10 : 0,
      total: 0,
    },
    payment: {
      method: ['credit', 'debit', 'paypal', 'apple_pay'][index % 4],
      status: 'completed',
      transactionId: `TXN-${Date.now()}-${index}`,
    },
    shipping: {
      method: ['standard', 'express', 'overnight'][index % 3],
      carrier: ['USPS', 'UPS', 'FedEx'][index % 3],
      trackingNumber: `1Z${Math.random().toString(36).slice(2, 12).toUpperCase()}`,
      estimatedDelivery: new Date(Date.now() + 86400000 * (index % 7 + 1)).toISOString(),
    },
    status: ['pending', 'processing', 'shipped', 'delivered'][index % 4],
    notes: Array.from(
      { length: 5 },
      (_, i) => `Note ${i + 1}: This is a note about the order with additional details.`
    ),
    history: Array.from({ length: 5 }, (_, i) => ({
      timestamp: new Date(Date.now() - 86400000 * i).toISOString(),
      action: ['created', 'updated', 'shipped', 'delivered', 'reviewed'][i],
      user: `admin${i}`,
    })),
    metadata: {
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      version: 1,
      source: 'web',
      campaign: index % 5 === 0 ? 'summer_sale' : null,
    },
  };
}

/**
 * Generate documents of a specific approximate size in bytes
 */
export function generateDocOfSize(index: number, targetBytes: number): Record<string, unknown> {
  const baseDoc = {
    _id: `doc-${index}`,
    index,
    createdAt: new Date().toISOString(),
  };

  // Calculate padding needed
  const baseSize = JSON.stringify(baseDoc).length;
  const paddingNeeded = Math.max(0, targetBytes - baseSize - 20); // Account for padding field name

  return {
    ...baseDoc,
    padding: 'x'.repeat(paddingNeeded),
  };
}
